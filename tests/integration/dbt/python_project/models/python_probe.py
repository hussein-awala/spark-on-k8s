from __future__ import annotations


def model(dbt, session):
    """This separate project must fail before modifying the existing SQL table."""
    return session.range(123)
