from __future__ import annotations

import json


def test_handle_exception(caplog):
    from spark_on_k8s.api.utils import handle_exception

    e = Exception("test exception")
    response = handle_exception(e, status_code=500)
    assert len(caplog.messages) == 1
    exception_uuid = caplog.messages[0].split()[1][:-1]
    assert response.status_code == 500
    assert json.loads(response.body) == {
        "schema_version": 1,
        "error": {
            "code": "internal_error",
            "message": f"The query failed with exception {exception_uuid},"
            " please contact the administrator for more information.",
        },
    }
