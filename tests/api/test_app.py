from __future__ import annotations

from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from spark_on_k8s.api.app import router


@pytest.mark.parametrize("query, force", [("", False), ("?force=false", False), ("?force=true", True)])
def test_delete_app_forwards_force(query, force):
    app = FastAPI()
    app.include_router(router)
    with mock.patch("spark_on_k8s.api.app.AsyncSparkAppManager") as manager_class:
        manager_class.return_value.delete_app = mock.AsyncMock()

        response = TestClient(app).delete(f"/app/test-namespace/test-app{query}")

    assert response.status_code == 200
    manager_class.return_value.delete_app.assert_awaited_once_with(
        namespace="test-namespace", app_id="test-app", force=force
    )
