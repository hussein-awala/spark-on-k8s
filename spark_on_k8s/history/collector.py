from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC
from uuid import uuid4

from kubernetes_asyncio.client import CoreV1Api

from spark_on_k8s.history.store import STATUSES, LostLease

logger = logging.getLogger(__name__)


def metadata_from_pod(pod, namespace):
    metadata = pod.metadata
    if metadata is None or not metadata.uid or not metadata.name or metadata.namespace != namespace:
        raise ValueError("Incomplete or mismatched driver metadata")
    labels = metadata.labels or {}
    if labels.get("spark-role") != "driver":
        raise ValueError("Unexpected pod in driver snapshot")
    containers = pod.spec.containers if pod.spec else []
    selected = (metadata.annotations or {}).get("kubectl.kubernetes.io/default-container", "driver")
    driver = next(
        (item for item in containers if item.name == selected), containers[0] if containers else None
    )
    phase = pod.status.phase if pod.status else None
    created = metadata.creation_timestamp
    return {
        "uid": str(metadata.uid),
        "namespace": namespace,
        "app_id": labels.get("spark-app-id"),
        "name": labels.get("spark-app-name") or labels.get("spark-app-id") or metadata.name,
        "driver_pod": metadata.name,
        "image": driver.image if driver else None,
        "spark_version": labels.get("spark-version"),
        "status": phase if phase in STATUSES else "Unknown",
        "created_at": created.replace(tzinfo=created.tzinfo or UTC)
        .astimezone(UTC)
        .isoformat()
        .replace("+00:00", "Z")
        if created
        else None,
    }


class RunCollector:
    def __init__(self, settings, store, client_provider):
        self.settings, self.store, self.client_provider = settings, store, client_provider
        self.owner = uuid4().hex
        self.lease = None
        self.task = None
        self.stop_event = asyncio.Event()
        self.next_namespace = 0

    async def start(self):
        await asyncio.to_thread(self.store.initialize)
        self.task = asyncio.create_task(self._run(), name="spark-run-history")

    async def stop(self):
        self.stop_event.set()
        if self.task is not None:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None
        if self.lease is not None:
            try:
                await asyncio.to_thread(self.store.release, self.lease)
            except Exception:
                # The lease expires independently; release failure must not block other client cleanup.
                logger.warning("Run-history collector lease could not be released during shutdown")
            finally:
                self.lease = None

    async def _run(self):
        while not self.stop_event.is_set():
            try:
                await self.collect_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Pod/API payloads can contain secrets. Keep collection errors generic.
                logger.warning("Run-history collection could not complete")
            try:
                await asyncio.wait_for(self.stop_event.wait(), self.settings.interval)
            except TimeoutError:
                pass

    async def _namespace(self, namespace):
        api = CoreV1Api(await self.client_provider())
        records, cursors = [], set()
        cursor = None
        for _ in range(self.settings.max_pages):
            options = {
                "namespace": namespace,
                "label_selector": "spark-role=driver",
                "limit": self.settings.page_size,
                "_request_timeout": self.settings.request_timeout,
            }
            if cursor:
                options["_continue"] = cursor
            page = await api.list_namespaced_pod(**options)
            records.extend(metadata_from_pod(pod, namespace) for pod in page.items)
            cursor = page.metadata._continue if page.metadata else None
            if not cursor:
                return records
            if cursor in cursors:
                raise ValueError("Repeated Kubernetes pagination cursor")
            cursors.add(cursor)
        raise ValueError("Driver snapshot exceeds the configured page limit")

    async def collect_once(self):
        lease = await asyncio.to_thread(self.store.acquire, self.owner, self.settings.lease_seconds)
        if lease is None:
            return False
        self.lease = lease
        deadline = time.monotonic() + self.settings.collection_timeout
        start = self.next_namespace
        self.next_namespace = (start + 1) % len(self.settings.namespaces)
        ordered = self.settings.namespaces[start:] + self.settings.namespaces[:start]
        for namespace in ordered:
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                records = await asyncio.wait_for(self._namespace(namespace), remaining)
                await asyncio.to_thread(self.store.snapshot, namespace, records, lease)
            except asyncio.CancelledError:
                raise
            except LostLease:
                return False
            except Exception:
                try:
                    await asyncio.to_thread(self.store.failure, namespace, lease)
                except LostLease:
                    return False
        return True
