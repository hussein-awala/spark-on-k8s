from __future__ import annotations

import math
import re
from decimal import Decimal

from kubernetes.utils.quantity import parse_quantity


def _number(value):
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError("Expected a finite nonnegative resource quantity")
    return result


def _memory(value, default_unit="m"):
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([bkmgt]?)", str(value).lower().replace(" ", ""))
    if match is None:
        raise ValueError("Unknown Spark memory unit")
    return _number(
        _number(match[1])
        * {"b": 2**-20, "k": 2**-10, "m": 1, "g": 1024, "t": 1024**2}[match[2] or default_unit]
    )


def resource_envelope(pod, conf):
    """Summarize explicit CPU/memory requests, never invent capacity for unsupported templates."""
    unknown = []
    driver = executor = counts = None
    spec = pod["spec"]
    try:
        requests = spec["containers"][0]["resources"]["requests"]
        driver = {
            "cpu_cores": _number(parse_quantity(requests["cpu"])),
            "memory_mib": _number(parse_quantity(requests["memory"]) / Decimal(2**20)),
        }
    except (KeyError, ValueError, TypeError, ArithmeticError):
        unknown.append("Driver requests cannot be quantified.")
    if len(spec.get("containers", [])) != 1 or spec.get("initContainers") or spec.get("overhead"):
        unknown.append("Driver sidecars, init containers or pod overhead require a separate capacity review.")
    if "spark.kubernetes.executor.podTemplateFile" in conf:
        unknown.append("Executor template resources are not included in the executor estimate.")
    try:
        heap = _memory(conf["spark.executor.memory"])
        overhead = _memory(conf["spark.executor.memoryOverhead"])
        off_heap_enabled = str(conf.get("spark.memory.offHeap.enabled", "false")).lower()
        if off_heap_enabled not in {"true", "false"}:
            raise ValueError("Invalid off-heap allocation flag")
        off_heap = (
            _memory(conf.get("spark.memory.offHeap.size", "0"), default_unit="b")
            if off_heap_enabled == "true"
            else 0
        )
        pyspark = _memory(conf.get("spark.executor.pyspark.memory", "0"))
        executor = {
            "cpu_cores": _number(
                parse_quantity(
                    str(conf.get("spark.kubernetes.executor.request.cores", conf["spark.executor.cores"]))
                )
            ),
            "memory_mib": _number(heap + overhead + off_heap + pyspark),
            "heap_mib": heap,
            "overhead_mib": overhead,
            "off_heap_mib": off_heap,
            "pyspark_mib": pyspark,
        }
        dynamic = str(conf.get("spark.dynamicAllocation.enabled", "false")).lower()
        if dynamic not in {"true", "false"}:
            raise ValueError("Invalid dynamic allocation flag")

        def count(key, default):
            value = int(str(conf.get(key, default)))
            if value < 0:
                raise ValueError("Invalid executor count")
            return value

        if dynamic == "true":
            minimum = count("spark.dynamicAllocation.minExecutors", 0)
            initial = max(
                minimum,
                count("spark.dynamicAllocation.initialExecutors", minimum),
                count("spark.executor.instances", 0),
            )
            maximum = (
                count("spark.dynamicAllocation.maxExecutors", 0)
                if "spark.dynamicAllocation.maxExecutors" in conf
                else None
            )
            if maximum is not None and maximum < initial:
                raise ValueError("Inconsistent executor bounds")
        else:
            minimum = initial = maximum = count("spark.executor.instances", 2)
        counts = {"minimum": minimum, "initial": initial, "maximum": maximum}
    except (KeyError, ValueError, TypeError, ArithmeticError):
        unknown.append("Executor configuration cannot be quantified reliably.")
    totals = {}
    for phase in ("minimum", "initial", "maximum"):
        count_value = None if counts is None else counts[phase]
        totals[phase] = None
        if not unknown and driver is not None and executor is not None and count_value is not None:
            try:
                totals[phase] = {
                    key: _number(driver[key] + count_value * executor[key])
                    for key in ("cpu_cores", "memory_mib")
                }
            except (ValueError, ArithmeticError):
                unknown.append("Aggregate resource requests exceed the supported numeric range.")
    if counts is not None and counts["maximum"] is None:
        unknown.append("Dynamic allocation has no explicit maximum; peak capacity is unknown.")
    return {
        "driver_requests": driver,
        "executor_requests": executor,
        "executor_counts": counts,
        "total_requests": totals,
        "unknown": unknown,
        "scope": (
            "CPU and memory requests before admission; excludes quota availability, storage and accelerators."
        ),
    }


def check_budget(envelope, *, max_cpu=None, max_memory_mib=None):
    """Evaluate caller-supplied maximum request budgets; unknown peaks cannot pass."""
    limits = {
        key: value
        for key, value in {"cpu_cores": max_cpu, "memory_mib": max_memory_mib}.items()
        if value is not None
    }
    for value in limits.values():
        if _number(value) <= 0:
            raise ValueError("Budget limits must be positive")
    maximum = envelope["total_requests"]["maximum"]
    violations = (
        ["Maximum resource requests are unknown"]
        if limits and maximum is None
        else [
            f"Maximum {key} exceeds the supplied budget"
            for key, value in limits.items()
            if maximum[key] > value
        ]
    )
    return {"limits": limits, "passed": not violations, "violations": violations}
