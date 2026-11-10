from __future__ import annotations

import uuid
from http import HTTPStatus
from typing import TYPE_CHECKING

from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse, Response

from spark_on_k8s.api.audit import REQUEST_ID_SCOPE_KEY, note

if TYPE_CHECKING:
    from fastapi import FastAPI
    from starlette.requests import Request

# Codes for errors raised without one, including Starlette's routing 404 and 405.
_STATUS_CODES = {
    400: "invalid_request",
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    405: "method_not_allowed",
    408: "request_timeout",
    409: "conflict",
    412: "not_configured",
    413: "body_too_large",
    415: "invalid_content_type",
    422: "invalid_request",
    429: "too_many_requests",
    502: "upstream_unavailable",
    503: "unavailable",
    504: "upstream_timeout",
}


def error_body(code: str, message: str) -> dict:
    """The API's single error envelope, shared with the versioned /jobs/v1 contract."""
    return {"schema_version": 1, "error": {"code": code, "message": message}}


def handle_exception(e: Exception, status_code: int = 500) -> Response:
    from spark_on_k8s.api import logger

    exception_uuid = uuid.uuid4()
    logger.exception("Exception %s: %s", exception_uuid, e)
    note(code="internal_error")
    return JSONResponse(
        error_body(
            "internal_error",
            f"The query failed with exception {exception_uuid},"
            f" please contact the administrator for more information.",
        ),
        status_code=status_code,
    )


async def _http_error(request: Request, error: HTTPException) -> Response:
    detail = error.detail
    if isinstance(detail, dict) and all(isinstance(detail.get(key), str) for key in ("code", "message")):
        code, message = detail["code"], detail["message"]
    else:
        code = _STATUS_CODES.get(error.status_code, "error")
        message = detail if isinstance(detail, str) and detail else HTTPStatus(error.status_code).phrase
    note(code=code)
    if error.status_code in {204, 304}:
        return Response(status_code=error.status_code, headers=error.headers)
    return JSONResponse(error_body(code, message), status_code=error.status_code, headers=error.headers)


async def _validation_error(request: Request, error: RequestValidationError) -> Response:
    problems = [
        f"{'.'.join(str(part) for part in item.get('loc', ()))}: {item.get('msg', 'invalid value')}"
        for item in error.errors()
    ]
    message = "; ".join(problems) or "Invalid request."
    note(code="invalid_request")
    return JSONResponse(error_body("invalid_request", message), status_code=422)


async def _unexpected_error(request: Request, error: Exception) -> Response:
    response = handle_exception(error, 500)
    # This handler answers outside the middleware stack, so it adds the request ID itself.
    if request_id := request.scope.get(REQUEST_ID_SCOPE_KEY):
        response.headers["X-Request-ID"] = request_id
    return response


def install_error_handlers(app: FastAPI) -> None:
    """Serialize every HTTP error, including framework routing and validation errors, alike."""
    app.add_exception_handler(HTTPException, _http_error)
    app.add_exception_handler(RequestValidationError, _validation_error)
    app.add_exception_handler(Exception, _unexpected_error)
