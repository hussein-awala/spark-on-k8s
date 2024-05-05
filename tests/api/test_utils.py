from __future__ import annotations


def test_handle_exception(caplog):
    from spark_on_k8s.api.utils import handle_exception

    e = Exception("test exception")
    response = handle_exception(e, status_code=500)
    assert len(caplog.messages) == 1
    exception_uuid = caplog.messages[0].split()[1][:-1]
    assert response.status_code == 500
    assert response.body == (
        b"The query failed with exception "
        + exception_uuid.encode()
        + b", please contact the administrator for more information."
    )
