from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from kubernetes import client as k8s

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


class _UniqueKeyLoader(yaml.SafeLoader):
    """Reject ambiguous YAML instead of silently accepting the last duplicate key."""


def _construct_mapping(loader, node, deep=False):
    loader.flatten_mapping(node)
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if not isinstance(key, str):
            raise ValueError("Driver pod template keys must be strings")
        if key in result:
            raise ValueError(f"Driver pod template contains duplicate key {key!r}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueKeyLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping)


def load_driver_pod_template(template: str | Path | Mapping[str, Any]) -> dict[str, Any]:
    """Load a local Pod YAML file, YAML text, or mapping without changing the input."""
    from collections.abc import Mapping

    if isinstance(template, Mapping):
        pod = deepcopy(dict(template))
    else:
        if isinstance(template, Path):
            try:
                content = template.expanduser().read_text(encoding="utf-8")
            except OSError as exc:
                raise ValueError(f"Cannot read driver pod template file: {template}") from exc
        elif isinstance(template, str):
            try:
                path = Path(template).expanduser()
                is_file = "\n" not in template and path.is_file()
            except OSError:
                is_file = False
            if is_file:
                try:
                    content = path.read_text(encoding="utf-8")
                except OSError as exc:
                    raise ValueError(f"Cannot read driver pod template file: {template}") from exc
            else:
                content = template
        else:
            raise ValueError("Driver pod template must be YAML text, a local path, or a mapping")
        try:
            pod = yaml.load(content, Loader=_UniqueKeyLoader)
        except yaml.YAMLError as exc:
            raise ValueError("Driver pod template contains invalid YAML") from exc
    _validate_template(pod)
    return pod


def _mapping(value: Any, field: str) -> dict:
    if not isinstance(value, dict):
        raise ValueError(f"Driver pod template {field} must be an object")
    return value


def _items(value: Any, field: str) -> list[dict]:
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise ValueError(f"Driver pod template {field} must be a list of objects")
    return value


def _field_key(field: str) -> Callable[[dict], str]:
    def key(item: dict) -> str:
        value = item.get(field)
        if not isinstance(value, str) or not value:
            raise ValueError(f"Driver pod template list entries require a nonempty {field}")
        return value

    return key


def _merge_items(base: list, override: list, key: Callable[[dict], Any]) -> list:
    # Replace an entire matching entry: recursive merges could combine incompatible
    # env value/valueFrom or volume source types. Keep the template's stable order.
    merged = {}
    for items in (base, override):
        seen = set()
        for item in items:
            identity = key(item)
            if identity in seen:
                raise ValueError(f"Driver pod template contains duplicate list entry {identity!r}")
            seen.add(identity)
            merged[identity] = deepcopy(item)
    return list(merged.values())


def _validate_template(pod: Any, *, allow_ownership: bool = False) -> None:
    _mapping(pod, "document")
    if pod.get("apiVersion") != "v1" or pod.get("kind") != "Pod":
        raise ValueError("Driver pod template must have apiVersion: v1 and kind: Pod")
    if set(pod) - {"apiVersion", "kind", "metadata", "spec"}:
        raise ValueError("Driver pod template may contain only apiVersion, kind, metadata, and spec")
    metadata = _mapping(pod.get("metadata", {}), "metadata")
    for field in (
        "ownerReferences",
        "uid",
        "resourceVersion",
        "managedFields",
        "generateName",
        "creationTimestamp",
        "deletionTimestamp",
        "deletionGracePeriodSeconds",
        "generation",
    ):
        if field in metadata:
            raise ValueError(f"Driver pod template metadata.{field} is managed and must be omitted")
    for field in ("labels", "annotations"):
        entries = _mapping(metadata.get(field, {}), f"metadata.{field}")
        if any(not isinstance(key, str) or not isinstance(value, str) for key, value in entries.items()):
            raise ValueError(f"Driver pod template metadata.{field} must contain string keys and values")
    if not allow_ownership:
        for key in metadata.get("annotations", {}):
            namespace, separator, _ = key.partition("/")
            if namespace == "spark-on-k8s.io" and separator:
                raise ValueError(
                    "Driver pod template cannot set reserved spark-on-k8s.io/ ownership annotations"
                )
    spec = _mapping(pod.get("spec"), "spec")
    containers = _items(spec.get("containers"), "spec.containers")
    if not containers:
        raise ValueError("Driver pod template must contain at least one container")
    for field in ("volumes", "initContainers", "imagePullSecrets"):
        _merge_items([], _items(spec.get(field, []), f"spec.{field}"), _field_key("name"))
    for field in ("nodeSelector", "securityContext", "affinity", "dnsConfig"):
        if field in spec:
            _mapping(spec[field], f"spec.{field}")
    all_containers = containers + spec.get("initContainers", [])
    _merge_items([], all_containers, _field_key("name"))
    for container in all_containers:
        for field, key in (("env", "name"), ("volumeMounts", "mountPath")):
            _merge_items([], _items(container.get(field, []), field), _field_key(key))
        _items(container.get("envFrom", []), "envFrom")
        _items(container.get("ports", []), "ports")
        for field in ("securityContext", "resources"):
            if field in container:
                _mapping(container[field], field)
        for field in ("requests", "limits"):
            if field in container.get("resources", {}):
                _mapping(container["resources"][field], f"resources.{field}")


def merge_driver_pod_template(
    generated: k8s.V1PodTemplateSpec,
    template: dict[str, Any],
    driver_container_name: str | None = None,
) -> dict[str, Any]:
    """Overlay a generated driver on a validated Pod template, preserving other fields.

    The returned API dictionary preserves Kubernetes fields not modeled by this client
    version. Validation here covers template shape and merge ambiguity; the API server
    remains responsible for Kubernetes schema, admission, and cluster policy checks.
    """
    _validate_template(template)
    pod = deepcopy(template)
    with k8s.ApiClient() as api_client:
        managed = api_client.sanitize_for_serialization(generated)
    containers = pod["spec"]["containers"]
    names = [container["name"] for container in containers]
    selected = driver_container_name
    if selected is None:
        selected = "driver" if "driver" in names else names[0] if len(names) == 1 else None
    if selected not in names:
        raise ValueError(
            "Select an existing driver container with driver_container_name; "
            "selection is ambiguous or missing"
        )
    for container in containers:
        if container["name"] != selected and not container.get("image"):
            raise ValueError(f"Sidecar {container['name']!r} requires an image")
    driver = next(container for container in containers if container["name"] == selected)
    generated_driver = managed["spec"]["containers"][0]
    driver.pop("command", None)  # Spark images must use their driver entrypoint.
    for field in ("image", "imagePullPolicy", "args"):
        driver[field] = generated_driver[field]
    resources = driver.setdefault("resources", {})
    for field in ("requests", "limits"):
        resources[field] = {**resources.get(field, {}), **generated_driver["resources"].get(field, {})}
    for field, key in (("env", "name"), ("volumeMounts", "mountPath")):
        driver[field] = _merge_items(driver.get(field, []), generated_driver.get(field, []), _field_key(key))
    driver["ports"] = _merge_items(
        driver.get("ports", []),
        generated_driver.get("ports", []),
        lambda port: port.get("name") or (port.get("containerPort"), port.get("protocol", "TCP")),
    )
    driver["envFrom"] = deepcopy(driver.get("envFrom", []))
    for source in generated_driver.get("envFrom", []):
        if source not in driver["envFrom"]:
            driver["envFrom"].append(source)
    spec = pod["spec"]
    spec["containers"] = [driver, *(container for container in containers if container["name"] != selected)]
    spec.pop("serviceAccount", None)
    for field in ("serviceAccountName", "restartPolicy"):
        spec[field] = managed["spec"][field]
    # An explicit max_runtime wins; otherwise a template deadline is kept like other pod fields.
    if "activeDeadlineSeconds" in managed["spec"]:
        spec["activeDeadlineSeconds"] = managed["spec"]["activeDeadlineSeconds"]
    spec["nodeSelector"] = {**spec.get("nodeSelector", {}), **managed["spec"].get("nodeSelector", {})}
    for field, key in (("volumes", "name"), ("initContainers", "name"), ("hostAliases", "ip")):
        spec[field] = _merge_items(
            _items(spec.get(field, []), field), managed["spec"].get(field, []), _field_key(key)
        )
    spec["tolerations"] = _items(spec.get("tolerations", []), "tolerations")
    for toleration in managed["spec"].get("tolerations", []):
        if toleration not in spec["tolerations"]:
            spec["tolerations"].append(toleration)
    metadata = pod.setdefault("metadata", {})
    for field in ("name", "namespace"):
        metadata[field] = managed["metadata"][field]
    labels = metadata.setdefault("labels", {})
    for key in ("spark-app-name", "spark-app-id", "spark-role", "spark-ui-proxy"):
        labels.pop(key, None)
    labels.update(managed["metadata"]["labels"])
    metadata["annotations"] = {
        **metadata.get("annotations", {}),
        **managed["metadata"].get("annotations", {}),
        "kubectl.kubernetes.io/default-container": selected,
    }
    _validate_template(pod, allow_ownership=True)
    return pod
