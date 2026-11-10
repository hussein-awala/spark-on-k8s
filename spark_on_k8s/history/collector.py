from __future__ import annotations

import asyncio
import contextlib
import logging
import sqlite3
import time
from datetime import UTC
from uuid import uuid4

from aiohttp import ClientError
from kubernetes_asyncio.client import ApiException, CoreV1Api

from spark_on_k8s.history.store import STATUSES, LostLease

logger = logging.getLogger(__name__)
_API_ERRORS = {401: "unauthorized", 403: "forbidden", 404: "not_found", 410: "pagination", 429: "throttled"}


class CollectionError(ValueError):
    """A refused namespace snapshot with a bounded, non-sensitive failure category."""

    def __init__(self, message, category):
        super().__init__(message)
        self.category = category


def error_category(error: BaseException) -> str:
    """Classify a failure by type and status only: Kubernetes and SQLite messages can contain secrets."""
    if isinstance(error, ApiException):
        status = error.status or 0
        return _API_ERRORS.get(status, "server_error" if status >= 500 else "api_error")
    if isinstance(error, TimeoutError):
        return "timeout"
    if isinstance(error, CollectionError):
        return error.category
    if isinstance(error, (ValueError, sqlite3.IntegrityError)):
        return "invalid_metadata"
    if isinstance(error, sqlite3.Error):
        return "store"
    if isinstance(error, (ClientError, OSError)):
        return "connection"
    return "unexpected"


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
        self.stale = set()

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
                raise CollectionError("Repeated Kubernetes pagination cursor", "pagination")
            cursors.add(cursor)
        raise CollectionError("Driver snapshot exceeds the configured page limit", "page_limit")

    async def collect_once(self):
        acquiring = asyncio.ensure_future(
            asyncio.to_thread(self.store.acquire, self.owner, self.settings.lease_seconds)
        )
        try:
            lease = await asyncio.shield(acquiring)
        except asyncio.CancelledError:
            # Cancellation does not stop the thread, which can still commit the lease.
            # Wait for it, so that stop() releases that lease instead of leaving it to expire.
            with contextlib.suppress(Exception):
                self.lease = await acquiring or self.lease
            raise
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
            except Exception as error:
                try:
                    await asyncio.to_thread(self.store.failure, namespace, lease, error_category(error))
                except LostLease:
                    return False
        await self._report_freshness()
        return True

    async def _report_freshness(self):
        """Log when a namespace becomes stale or fresh again, once per transition for this collector."""
        health = await asyncio.to_thread(self.store.health)
        now = self.store.clock()
        for namespace in self.settings.namespaces:
            state = health.get(namespace, {})
            last_success = state.get("last_success")
            if self.settings.is_fresh(last_success, now):
                if namespace in self.stale:
                    self.stale.discard(namespace)
                    logger.info("Run-history collection for namespace %s is fresh again", namespace)
            elif namespace not in self.stale:
                self.stale.add(namespace)
                logger.warning(
                    "Run-history collection for namespace %s is stale: last success %s; last error: %s",
                    namespace,
                    "never" if last_success is None else f"{max(0, now - last_success):.0f}s ago",
                    # No failure since the last success: the collection budget expired before this namespace.
                    state.get("last_error") or "not_attempted",
                )
