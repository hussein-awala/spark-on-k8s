"""Drive the example gateway like a browser, from a pod that is not a gateway pod.

Standard library only: check.sh runs this file with the API image's interpreter.
SSO_PREFIX selects the public path prefix of the deployment under test.
"""

from __future__ import annotations

import base64
import hashlib
import http.client
import json
import os
import re
import socket
from http.cookies import SimpleCookie
from urllib.parse import parse_qs, urlencode, urlsplit

PREFIX = os.environ.get("SSO_PREFIX", "")
PUBLIC_HOST = "spark.example.com"
ORIGIN = f"https://{PUBLIC_HOST}"
GATEWAY = ("spark-console-gateway.spark-system.svc.cluster.local", 80)
API = ("spark-console-spark-on-k8s.spark-system.svc.cluster.local", 8000)
ISSUER = "http://mock-oidc.sso-fixtures.svc.cluster.local:8080/default"
# __Host- cookies require Path=/, so the prefixed deployment scopes a __Secure- cookie instead.
SESSION = "__Secure-spark-console" if PREFIX else "__Host-spark-console"
LOGS = f"{PREFIX}/webserver/ws/logs/sso-fixtures/missing-app?tail=10"
CHECKS = []


def check(name, condition, detail=None):
    CHECKS.append(name)
    assert condition, f"{name}: {detail!r}"


def fetch(method, url, *, cookies=None, headers=None, body=None):
    """One request without redirects; bare paths go to the gateway Service with the public Host."""
    target = urlsplit(url)
    address = (target.hostname, target.port or 80) if target.hostname else GATEWAY
    sent = {"Host": target.netloc or PUBLIC_HOST, **(headers or {})}
    if cookies:
        sent["Cookie"] = "; ".join(f"{name}={value}" for name, value in cookies.items())
    connection = http.client.HTTPConnection(*address, timeout=30)
    try:
        path = target.path + (f"?{target.query}" if target.query else "")
        connection.request(method, path, body=body, headers=sent)
        response = connection.getresponse()
        return response.status, response.headers, response.read()
    finally:
        connection.close()


def store(jar, headers):
    raw = headers.get_all("Set-Cookie") or []
    for line in raw:
        for name, morsel in SimpleCookie(line).items():
            if morsel.value and morsel["max-age"] not in {"0", "-1"}:
                jar[name] = morsel.value
            else:
                jar.pop(name, None)
    return raw


def login(user):
    jar = {}
    status, headers, _ = fetch("GET", f"{PREFIX}/webserver/apps", headers={"Accept": "text/html"})
    check("anonymous page starts sign-in", status == 302, status)
    authorize = headers["Location"]
    query = parse_qs(urlsplit(authorize).query)
    check("redirects to issuer", authorize.startswith(f"{ISSUER}/authorize?"), authorize)
    check("uses PKCE S256 and nonce", query["code_challenge_method"] == ["S256"] and "nonce" in query, query)
    check("registered callback", query["redirect_uri"] == [f"{ORIGIN}{PREFIX}/oauth2/callback"], query)
    store(jar, headers)
    status, _, page = fetch("GET", authorize)
    check("identity provider login form", status == 200 and b"<form" in page, status)
    form = urlencode({"username": user}).encode()
    form_headers = {"Content-Type": "application/x-www-form-urlencoded"}
    status, headers, _ = fetch("POST", authorize, headers=form_headers, body=form)
    callback = urlsplit(headers.get("Location", ""))
    returned = (status, callback.path)
    check("provider returns to callback", returned == (302, f"{PREFIX}/oauth2/callback"), returned)
    status, headers, _ = fetch("GET", f"{callback.path}?{callback.query}", cookies=jar)
    return status, headers, store(jar, headers), jar


def websocket(path, *, origin=None, cookies=None):
    key = base64.b64encode(os.urandom(16)).decode()
    lines = [
        f"GET {path} HTTP/1.1",
        f"Host: {PUBLIC_HOST}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key}",
        "Sec-WebSocket-Version: 13",
    ]
    if origin:
        lines.append(f"Origin: {origin}")
    if cookies:
        lines.append("Cookie: " + "; ".join(f"{name}={value}" for name, value in cookies.items()))
    with socket.create_connection(GATEWAY, timeout=30) as connection:
        connection.sendall(("\r\n".join(lines) + "\r\n\r\n").encode())
        response = b""
        while b"\r\n\r\n" not in response and (chunk := connection.recv(4096)):
            response += chunk
    status_line, *fields = response.split(b"\r\n\r\n", 1)[0].decode("latin-1").split("\r\n")
    status = int(status_line.split(" ", 2)[1])
    if status == 101:
        received = {name.strip().lower(): value.strip() for name, value in (f.split(":", 1) for f in fields)}
        digest = hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
        expected = base64.b64encode(digest).decode()
        check("websocket accept key", received.get("sec-websocket-accept") == expected, received)
    return status


def main():
    # The NetworkPolicy admits only gateway pods; this pod resolves but cannot reach the API.
    socket.getaddrinfo(*API)
    try:
        socket.create_connection(API, timeout=5).close()
    except OSError as error:
        check("network policy blocks a non-gateway pod", True, error)
    else:
        check("network policy blocks a non-gateway pod", False, "connected")

    # Anonymous requests: probe paths answered by the gateway, background calls get 401.
    for path in ("/health", "/ready"):
        status, _, body = fetch("GET", PREFIX + path)
        check(f"gateway answers {PREFIX}{path} itself", (status, body) == (200, b"OK"), (status, body))
    status, _, _ = fetch("GET", f"{PREFIX}/webserver/api/apps?namespace=sso-fixtures&limit=5")
    check("anonymous API route returns 401", status == 401, status)
    check("anonymous log stream refused", websocket(LOGS, origin=ORIGIN) == 401)

    status, _, raw, _ = login("mallory")
    check("user outside the allowed group is refused", status == 403, status)
    check("refused user gets no session", not any(c.startswith(f"{SESSION}=") for c in raw), raw)

    status, headers, raw, jar = login("alice")
    returned = (status, headers.get("Location"))
    check("allowed user returns to the console", returned == (302, f"{PREFIX}/webserver/apps"), returned)
    (session,) = [c for c in raw if c.startswith(f"{SESSION}=")]
    attributes = {part.strip().lower() for part in session.split(";")[1:]}
    expected = {f"path={PREFIX or '/'}", "secure", "httponly", "samesite=lax"}
    check("session cookie attributes", expected <= attributes, session)
    check("host-only session cookie", not any(a.startswith("domain=") for a in attributes), session)

    status, _, body = fetch("GET", f"{PREFIX}/", cookies=jar)
    check("authenticated request reaches the API", json.loads(body)["message"].startswith("Welcome"), body)
    for path in ("/health", "/ready"):
        status, _, body = fetch("GET", PREFIX + path, cookies=jar)
        check(f"API {path} is not proxied", (status, body) == (200, b"OK"), (status, body))
    status, headers, page = fetch("GET", f"{PREFIX}/webserver/apps", cookies=jar)
    check("console page", status == 200 and headers.get_content_type() == "text/html", status)
    check("console base path", f'"basePath": "{PREFIX}"'.encode() in page, page[:500])
    assets = re.findall(rb'(?:src|href)="(' + re.escape(PREFIX.encode()) + rb'/static/[^"]+)"', page)
    check("console references static assets", assets, page[:500])
    for asset in assets:
        status, _, _ = fetch("GET", asset.decode(), cookies=jar)
        check(f"static asset {asset.decode()}", status == 200, status)
    status, _, body = fetch("GET", f"{PREFIX}/webserver/api/apps?namespace=sso-fixtures&limit=5", cookies=jar)
    check("console API", status == 200 and "items" in json.loads(body), (status, body[:200]))

    # The Spark UI proxy must not pass browser credentials or gateway/client identity upstream.
    forged = {
        "Accept-Language": "fr",
        "Authorization": "Bearer forged",
        "X-Forwarded-Email": "forged@example.com",
        "X-Forwarded-User": "forged",
    }
    echo = f"{PREFIX}/webserver/ui/sso-fixtures/header-echo/"
    status, _, body = fetch("GET", echo, cookies=jar, headers=forged)
    received = set(json.loads(body)) if status == 200 else set()
    check("Spark UI proxy reaches the upstream", "accept-language" in received, (status, body[:200]))
    leaked = {
        name
        for name in received
        if name in {"cookie", "authorization", "proxy-authorization", "forwarded", "gap-auth", "x-real-ip"}
        or name.startswith(("x-forwarded-", "x-auth-request-"))
    }
    check("no credentials or identity reach the Spark UI", not leaked, leaked)

    check("log stream upgrade through the gateway", websocket(LOGS, origin=ORIGIN, cookies=jar) == 101)
    other = websocket(LOGS, origin="https://evil.example.net", cookies=jar)
    check("log stream from another origin refused", other == 403, other)
    print(json.dumps({"result": "passed", "prefix": PREFIX, "checks": CHECKS}, indent=2))


if __name__ == "__main__":
    main()
