from __future__ import annotations

import http.client
import ipaddress
import json
import math
import socket
import ssl
import threading
import time
from urllib.parse import urlsplit

MAX_RESPONSE_BYTES = 4 * 1024 * 1024


class RemoteError(Exception):
    """A sanitized failure from the remote API or transport."""

    def __init__(self, code: str, message: str, *, status: int | None = None):
        super().__init__(message)
        self.code = code
        self.status = status


class RemoteClient:
    """Bounded, authenticated requests without local Kubernetes credentials or automatic retries."""

    def __init__(self, api_url: str, token: str, *, timeout: float = 30):
        url = urlsplit(api_url)
        try:
            loopback = ipaddress.ip_address(url.hostname or "").is_loopback
        except ValueError:
            loopback = url.hostname == "localhost"
        if (
            url.scheme not in {"https", "http"}
            or not url.hostname
            or url.username is not None
            or url.password is not None
            or url.query
            or url.fragment
            or url.scheme == "http"
            and not loopback
        ):
            raise ValueError(
                "Use an HTTPS API URL without credentials, query or fragment; "
                "HTTP is allowed only on loopback."
            )
        if not isinstance(token, str) or not token or any(ord(c) < 33 or ord(c) > 126 for c in token):
            raise ValueError("A nonempty printable ASCII API token without spaces is required.")
        if not math.isfinite(timeout) or not 0 < timeout <= 300:
            raise ValueError("Remote timeout must be greater than zero and at most 300 seconds.")
        self._url, self._token, self.timeout = url, token, timeout

    def request(self, operation: str, document: dict) -> dict:
        if operation not in {"plan", "submit"}:
            raise ValueError("Unsupported remote operation.")
        payload = json.dumps(document, allow_nan=False).encode("utf-8")
        if len(payload) > 1024 * 1024:
            raise ValueError("Remote job documents cannot exceed 1 MiB.")
        connection_type = (
            http.client.HTTPSConnection if self._url.scheme == "https" else http.client.HTTPConnection
        )
        kwargs = {"context": ssl.create_default_context()} if self._url.scheme == "https" else {}
        connection = connection_type(self._url.hostname, self._url.port, timeout=self.timeout, **kwargs)
        deadline = time.monotonic() + self.timeout
        timer = None
        expired = threading.Event()
        try:
            connection.connect()
            transport = connection.sock

            def interrupt():
                expired.set()
                try:
                    transport.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError
            # Shut down the actual socket, including HTTP/1.0 bodies after HTTPConnection detaches it.
            timer = threading.Timer(remaining, interrupt)
            timer.daemon = True
            timer.start()
            connection.request(
                "POST",
                self._url.path.rstrip("/") + "/jobs/v1/" + operation,
                body=payload,
                headers={
                    "Authorization": "Bearer " + self._token,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
            response = connection.getresponse()
            if 300 <= response.status < 400:
                raise RemoteError(
                    "redirect_refused",
                    "The API redirected the request; redirects are not followed.",
                    status=response.status,
                )
            data = bytearray()
            while chunk := response.read1(65536):
                data.extend(chunk)
                if len(data) > MAX_RESPONSE_BYTES:
                    raise RemoteError("invalid_response", "The API response exceeded the size limit.")
            if expired.is_set() or time.monotonic() >= deadline:
                raise TimeoutError
            try:
                result = json.loads(data)
                if (
                    not isinstance(result, dict)
                    or type(result.get("schema_version")) is not int
                    or result.get("schema_version") != 1
                    or "error" not in result
                ):
                    raise ValueError
            except (ValueError, UnicodeError) as error:
                raise RemoteError(
                    "invalid_response",
                    "The API did not return a supported JSON result.",
                    status=response.status,
                ) from error
            if response.status >= 400:
                error = result.get("error")
                if not isinstance(error, dict) or not all(
                    isinstance(error.get(k), str) for k in ("code", "message")
                ):
                    raise RemoteError(
                        "invalid_response",
                        "The API returned an invalid error response.",
                        status=response.status,
                    )
                raise RemoteError(error["code"], error["message"], status=response.status)
            if (
                response.status != (200 if operation == "plan" else 201)
                or result.get("operation") != operation
                or result["error"] is not None
            ):
                raise RemoteError("invalid_response", "The API returned an unexpected operation result.")
            if operation == "submit":
                run = result.get("run")
                if not isinstance(run, dict) or not all(
                    isinstance(run.get(k), str) and run[k] for k in ("namespace", "app_id", "driver_pod")
                ):
                    raise RemoteError("invalid_response", "The API returned an invalid run receipt.")
            elif not isinstance(result.get("plan"), dict):
                raise RemoteError("invalid_response", "The API returned an invalid plan.")
            return result
        except (TimeoutError, OSError, http.client.HTTPException) as error:
            if expired.is_set() or isinstance(error, TimeoutError):
                raise RemoteError(
                    "timeout",
                    "Remote request timed out. A submission may still have succeeded; "
                    "inspect existing runs before retrying.",
                ) from error
            raise RemoteError(
                "connection_error",
                "The remote API connection failed. A submission may still have succeeded; "
                "inspect existing runs before retrying.",
            ) from error
        finally:
            if timer is not None:
                timer.cancel()
                timer.join()
            connection.close()
