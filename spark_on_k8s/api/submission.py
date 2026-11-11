from __future__ import annotations

import asyncio
import logging
from itertools import pairwise
from uuid import uuid4

from fastapi import APIRouter, Request
from kubernetes.client.exceptions import ApiException
from starlette.concurrency import run_in_threadpool
from starlette.requests import ClientDisconnect
from starlette.responses import JSONResponse

from spark_on_k8s.api.audit import count_kubernetes_error, note
from spark_on_k8s.api.namespace_scope import namespace_scope
from spark_on_k8s.api.submission_policy import (
    MAX_DOCUMENT_BYTES,
    AdmissionError,
    get_submission_settings,
    parse_json,
)
from spark_on_k8s.api.utils import error_body
from spark_on_k8s.client import SparkOnK8S

router = APIRouter(prefix="/jobs/v1", tags=["Remote jobs"])
logger = logging.getLogger(__name__)


def error_response(status, code, message):
    note(code=code)
    return JSONResponse(
        error_body(code, message),
        status_code=status,
        headers={"Cache-Control": "no-store"},
    )


def create_submission_client():
    """Use only server-configured credentials. Each operation has a separate run receipt."""
    return SparkOnK8S()


def _validate_plan(plan, options):
    # A last boundary against unexpected ambient defaults or new JobSpec fields.
    if plan.namespace != options["namespace"]:
        raise AdmissionError("The generated plan does not match the admitted namespace.")
    pods = []
    for resource in plan.resources:
        if resource["kind"] not in {"Pod", "Service"} or resource["metadata"]["namespace"] != plan.namespace:
            raise AdmissionError("The generated plan contains an unapproved resource.")
        if resource["kind"] == "Pod":
            pods.append(resource)
    if len(pods) != 1:
        raise AdmissionError("The generated plan must contain exactly one driver pod.")
    spec = pods[0]["spec"]
    containers = spec.get("containers", [])
    if (
        spec.get("serviceAccountName") != options["service_account"]
        or spec.get("activeDeadlineSeconds") != options.get("max_runtime")
        or len(containers) != 1
        or containers[0]["image"] != options["image"]
        or spec.get("volumes")
        or spec.get("initContainers")
        or containers[0].get("envFrom")
        or containers[0].get("volumeMounts")
        or spec.get("hostNetwork")
        or spec.get("hostPID")
        or spec.get("hostIPC")
        or spec.get("hostAliases")
        or spec.get("nodeSelector")
        or spec.get("tolerations")
        or spec.get("imagePullSecrets")
        or spec.get("securityContext")
        or containers[0].get("command")
        or containers[0].get("securityContext")
        or containers[0].get("workingDir")
        or containers[0].get("env")
        != [
            {
                "name": "SPARK_DRIVER_BIND_ADDRESS",
                "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}},
            }
        ]
    ):
        raise AdmissionError("The generated driver pod contains an unapproved setting.")
    _validate_metadata(plan, pods[0], options)


def _validate_metadata(plan, pod, options):
    # Admitted policy labels are the only labels a remote job adds; ownership labels stay generated.
    labels = dict(pod["metadata"].get("labels") or {})
    labels.pop("spark-app-name", None)
    prefix = "spark.kubernetes.executor.label."
    args = pod["spec"]["containers"][0].get("args") or []
    executor_labels = {}
    for flag, value in pairwise(args):
        if flag == "--conf" and value.startswith(prefix):
            key, _, label = value[len(prefix) :].partition("=")
            executor_labels[key] = label
    if (
        labels.pop("spark-app-id", None) != plan.app_id
        or labels != {**options["driver_labels"], "spark-role": "driver"}
        or pod["metadata"].get("annotations")
        or executor_labels != options["executor_labels"]
    ):
        raise AdmissionError("The generated driver pod contains unapproved labels or annotations.")


def _execute(document, preview):
    settings = get_submission_settings()
    options = settings.policy.admit(document)
    # Startup rejects policies outside the scope; keep the boundary local to each operation too.
    if not namespace_scope().allows(options["namespace"]):
        raise AdmissionError("The namespace is outside this API's namespace scope.")
    # Random run identities prevent same-name requests within one second from colliding.
    suffix = "-" + uuid4().hex
    options["app_id_suffix"] = lambda: suffix
    # Leave room for the full random suffix and the Kubernetes driver pod suffix.
    options["app_name"] = options["app_name"][:23].rstrip("-")
    client = create_submission_client()
    plan = client.plan_app(**options)
    _validate_plan(plan, options)
    if preview:
        note(namespace=plan.namespace)
        return {"schema_version": 1, "operation": "plan", "plan": plan.to_dict(), "error": None}
    # Record the identity before creating resources: a failure can leave some of them behind.
    note(namespace=plan.namespace, app_id=plan.app_id, driver_pod=plan.driver_pod)
    run = client.submit_run(**options)
    note(driver_pod=run.driver_pod, driver_uid=run.driver_uid)
    return {"schema_version": 1, "operation": "submit", "run": run.to_dict(), "status": None, "error": None}


async def _handle(request: Request, preview: bool):
    if not get_submission_settings().enabled:
        return error_response(404, "remote_disabled", "Remote submission is disabled.")
    if "idempotency-key" in request.headers:
        return error_response(
            400,
            "unsupported_idempotency",
            "Idempotency-Key is not supported. Do not retry a submission automatically.",
        )
    if request.headers.get("content-type", "").partition(";")[0].strip().lower() != "application/json":
        return error_response(415, "invalid_content_type", "Send a JSON job document.")

    async def read_document():
        data = bytearray()
        async for chunk in request.stream():
            if len(data) + len(chunk) > MAX_DOCUMENT_BYTES:
                raise OverflowError
            data.extend(chunk)
        return parse_json(data)

    try:
        document = await asyncio.wait_for(read_document(), timeout=15)
    except OverflowError:
        return error_response(413, "body_too_large", "Job documents cannot exceed 1 MiB.")
    except TimeoutError:
        return error_response(408, "request_timeout", "The job document was not received in time.")
    except ClientDisconnect:
        # The document was not received, so nothing was created. The client never sees this response;
        # it gives the audit event a rejected outcome instead of an unknown one.
        return error_response(400, "client_disconnected", "The job document was not received.")
    except (ValueError, UnicodeError, RecursionError):
        return error_response(400, "invalid_json", "Send a JSON object with unique keys and finite values.")
    try:
        result = await run_in_threadpool(_execute, document, preview)
    except AdmissionError as error:
        return error_response(403, "policy_denied", str(error))
    except ValueError as error:
        return error_response(422, "invalid_job", str(error))
    except ApiException as error:
        count_kubernetes_error(error)
        code = 409 if error.status == 409 else 502
        return error_response(
            code,
            "submission_error",
            "The Kubernetes operation failed; submission may be partial. "
            "Inspect existing runs before retrying.",
        )
    except Exception:
        logger.exception("Remote job operation failed")
        return error_response(
            500,
            "operation_error",
            "The operation failed; inspect existing runs before retrying a submission.",
        )
    return JSONResponse(result, status_code=200 if preview else 201, headers={"Cache-Control": "no-store"})


@router.post("/plan")
async def plan_job(request: Request):
    return await _handle(request, True)


@router.post("/submit", status_code=201)
async def submit_job(request: Request):
    return await _handle(request, False)
