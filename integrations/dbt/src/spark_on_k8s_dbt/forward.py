from __future__ import annotations

import re
import selectors
import subprocess
import threading
import time


class PortForward:
    """A loopback-only, invocation-owned kubectl tunnel with an OS-assigned port."""

    def __init__(self, config, namespace: str, pod: str, port: int):
        self.config = config
        self.namespace = namespace
        self.pod = pod
        self.port = port
        self.process = None
        self.reader = None

    def start(self, timeout: float) -> int:
        command = [
            "kubectl",
            "--kubeconfig",
            self.config.kube_config_file,
            "--context",
            self.config.kube_context,
            "--namespace",
            self.namespace,
            "port-forward",
            "--address",
            "127.0.0.1",
            "pod/" + self.pod,
            f":{self.port}",
        ]
        self.process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + timeout
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(self.process.stdout, selectors.EVENT_READ)
                buffer = b""
                while time.monotonic() < deadline:
                    if self.process.poll() is not None:
                        raise RuntimeError(
                            "kubectl port-forward exited before the Spark endpoint was reachable"
                        )
                    for key, _ in selector.select(min(0.2, max(0, deadline - time.monotonic()))):
                        chunk = key.fileobj.read1(4096)
                        if not chunk:
                            continue
                        buffer = (buffer + chunk)[-8192:]
                        match = re.search(rb"Forwarding from 127\.0\.0\.1:(\d+) ->", buffer)
                        if match:
                            self.reader = threading.Thread(target=self._drain, daemon=True)
                            self.reader.start()
                            return int(match[1])
            raise TimeoutError("Timed out opening the private Spark tunnel")
        except BaseException:
            self.close()
            raise

    def _drain(self):
        try:
            while self.process.stdout.read1(4096):
                pass
        except (OSError, ValueError):
            pass

    def close(self):
        if self.process is None:
            return
        try:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
        finally:
            if self.reader is not None:
                self.reader.join(timeout=5)
            self.process.stdout.close()
