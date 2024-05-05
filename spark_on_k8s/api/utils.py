from __future__ import annotations

import uuid

from starlette.responses import Response


def handle_exception(e: Exception, status_code: int = 500) -> Response:
    from spark_on_k8s.api import logger

    exception_uuid = uuid.uuid4()
    logger.exception("Exception %s: %s", exception_uuid, e)
    return Response(
        status_code=status_code,
        content=(
            f"The query failed with exception {exception_uuid},"
            f" please contact the administrator for more information."
        ),
    )
