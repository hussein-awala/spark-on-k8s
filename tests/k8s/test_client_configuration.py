from __future__ import annotations

import asyncio
import importlib
import json
from unittest.mock import AsyncMock, Mock

import pytest
from kubernetes import client as sync_k8s
from kubernetes_asyncio import client as async_k8s
from spark_on_k8s.k8s import async_client, sync_client
from spark_on_k8s.utils import configuration


@pytest.mark.parametrize("in_cluster", [False, True])
def test_sync_client_uses_loaded_configuration(monkeypatch, in_cluster):
    client_config = sync_k8s.Configuration()
    client_config.api_key = {"authorization": "Bearer test-token"}

    def load_config(**kwargs):
        assert kwargs["client_configuration"] is client_config
        kwargs["client_configuration"].host = "https://configured-cluster.example"

    loader = Mock(side_effect=load_config)
    loader_name = "load_incluster_config" if in_cluster else "load_kube_config"
    monkeypatch.setattr(sync_client.config, loader_name, loader)
    manager = sync_client.KubernetesClientManager(client_configuration=client_config, in_cluster=in_cluster)

    with manager.client() as api_client:
        assert api_client.configuration is client_config
        assert api_client.configuration.host == "https://configured-cluster.example"
        assert api_client.configuration.api_key["authorization"] == "Bearer test-token"
    loader.assert_called_once()


@pytest.mark.parametrize("in_cluster", [False, True])
def test_async_client_uses_loaded_configuration(monkeypatch, in_cluster):
    client_config = async_k8s.Configuration()
    client_config.api_key = {"authorization": "Bearer test-token"}

    def load_config(**kwargs):
        assert kwargs["client_configuration"] is client_config
        kwargs["client_configuration"].host = "https://configured-cluster.example"

    loader = Mock(side_effect=load_config) if in_cluster else AsyncMock(side_effect=load_config)
    loader_name = "load_incluster_config" if in_cluster else "load_kube_config"
    monkeypatch.setattr(async_client.config, loader_name, loader)
    manager = async_client.KubernetesAsyncClientManager(
        client_configuration=client_config, in_cluster=in_cluster
    )

    async def check_client():
        async with manager.client() as api_client:
            assert api_client.configuration is client_config
            assert api_client.configuration.host == "https://configured-cluster.example"
            assert api_client.configuration.api_key["authorization"] == "Bearer test-token"

    asyncio.run(check_client())
    loader.assert_called_once()


def test_async_manager_defaults_to_async_configuration(monkeypatch):
    sync_config = sync_k8s.Configuration(host="https://sync.example")
    async_config = async_k8s.Configuration(host="https://async.example")
    monkeypatch.setattr(async_client.Configuration, "SPARK_ON_K8S_CLIENT_CONFIG", sync_config)
    monkeypatch.setattr(async_client.Configuration, "SPARK_ON_K8S_ASYNC_CLIENT_CONFIG", async_config)

    manager = async_client.KubernetesAsyncClientManager()

    assert manager.client_configuration is async_config


@pytest.mark.parametrize("variable", ["SPARK_ON_K8S_CLIENT_CONFIG", "SPARK_ON_K8S_ASYNC_CLIENT_CONFIG"])
def test_environment_configuration_sets_constructor_fields(monkeypatch, variable):
    values = {"host": "https://configured-cluster.example", "api_key": {"authorization": "test-token"}}
    try:
        with monkeypatch.context() as patch:
            patch.setenv(variable, json.dumps(values))
            importlib.reload(configuration)
            client_config = getattr(configuration.Configuration, variable)
            assert client_config.host == values["host"]
            assert client_config.api_key == values["api_key"]
    finally:
        importlib.reload(configuration)
