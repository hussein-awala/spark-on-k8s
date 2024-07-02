"""
This is an example module that contains a class definition
used in the job submission example.
"""
from __future__ import annotations


def is_point_in_the_unit_circle(point: tuple[float, float]) -> bool:
    """Check if a point is inside the unit circle."""
    return point[0] ** 2 + point[1] ** 2 <= 1
