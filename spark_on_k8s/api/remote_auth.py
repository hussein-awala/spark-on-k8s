from __future__ import annotations

import hmac

from starlette.responses import JSONResponse

from spark_on_k8s.api.submission_policy import get_submission_settings
from spark_on_k8s.api.utils import error_body


class RemoteAccessMiddleware:
    """Remote mode protects every HTTP/WebSocket surface; only liveness remains public."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] not in {"http", "websocket"}:
            return await self.app(scope, receive, send)
        settings = get_submission_settings()
        path = scope["path"]
        root_path = scope.get("root_path", "").rstrip("/")
        if root_path and path.startswith(root_path + "/"):
            path = path[len(root_path) :]
        health = (
            scope["type"] == "http" and path in {"/health", "/ready"} and scope["method"] in {"GET", "HEAD"}
        )
        if settings.enabled and not health:
            values = [value for key, value in scope["headers"] if key.lower() == b"authorization"]
            scheme, _, credential = values[0].partition(b" ") if len(values) == 1 else (b"", b"", b"")
            if scheme.lower() != b"bearer" or not hmac.compare_digest(credential, settings.token):
                if scope["type"] == "websocket":
                    await send({"type": "websocket.close", "code": 1008})
                else:
                    response = JSONResponse(
                        error_body("unauthorized", "Bearer authentication is required."),
                        status_code=401,
                        headers={"WWW-Authenticate": "Bearer", "Cache-Control": "no-store"},
                    )
                    await response(scope, receive, send)
                return
            scope["spark_on_k8s.remote_authenticated"] = True
        await self.app(scope, receive, send)
