"""Unit tests for get_resources logic using offline Azure SDK stubs."""

import importlib
import os
import sys
import tempfile
import types
import unittest


def import_get_resources_with_stubs(repo_root: str):
    """Import get_resources with Azure SDK modules stubbed for offline unit tests."""
    azure = types.ModuleType("azure")
    identity = types.ModuleType("azure.identity")
    mgmt = types.ModuleType("azure.mgmt")
    resourcegraph = types.ModuleType("azure.mgmt.resourcegraph")
    resourcegraph_models = types.ModuleType("azure.mgmt.resourcegraph.models")

    class DummyCredential:
        pass

    class DummyResourceGraphClient:
        def __init__(self, credential=None):
            self.credential = credential

        def resources(self, _query):
            return types.SimpleNamespace(data=[])

    class DummyQueryRequest:
        def __init__(self, subscriptions=None, query=None):
            self.subscriptions = subscriptions
            self.query = query

    identity.DefaultAzureCredential = DummyCredential
    resourcegraph.ResourceGraphClient = DummyResourceGraphClient
    resourcegraph_models.QueryRequest = DummyQueryRequest

    sys.modules["azure"] = azure
    sys.modules["azure.identity"] = identity
    sys.modules["azure.mgmt"] = mgmt
    sys.modules["azure.mgmt.resourcegraph"] = resourcegraph
    sys.modules["azure.mgmt.resourcegraph.models"] = resourcegraph_models

    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    if "get_resources" in sys.modules:
        del sys.modules["get_resources"]
    return importlib.import_module("get_resources")


class TestGetResourcesUnit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    def test_render_resource_sub_types_yaml_sorted(self):
        module = import_get_resources_with_stubs(self.repo_root)
        rendered = module.render_resource_sub_types_yaml(
            {"microsoft.web/sites", "microsoft.keyvault/vaults"}
        )
        self.assertEqual(
            rendered,
            "resource_sub_types:\n"
            "  - microsoft.keyvault/vaults\n"
            "  - microsoft.web/sites\n",
        )

    def test_load_config_reads_yaml(self):
        module = import_get_resources_with_stubs(self.repo_root)
        with tempfile.TemporaryDirectory() as tmp_dir:
            cfg = os.path.join(tmp_dir, "cfg.yaml")
            with open(cfg, "w", encoding="utf-8") as f:
                f.write("subscriptions:\n  - sub-a\n")
            loaded = module.load_config(cfg)

        self.assertEqual(loaded["subscriptions"], ["sub-a"])

    def test_get_deployed_resource_types_normalizes_and_dedupes(self):
        module = import_get_resources_with_stubs(self.repo_root)

        class RowObject:
            def __init__(self, value):
                self.type = value

        data_by_sub = {
            "sub1": [
                {"type": "Microsoft.Web/Sites"},
                RowObject("microsoft.web/sites"),
                {"type": "  MICROSOFT.KEYVAULT/VAULTS  "},
                {"type": ""},
                {"foo": "bar"},
            ],
            "sub2": [
                {"type": "microsoft.keyvault/vaults"},
                RowObject("microsoft.storage/storageaccounts"),
            ],
        }

        class FakeQueryRequest:
            def __init__(self, subscriptions=None, query=None):
                self.subscriptions = subscriptions
                self.query = query

        class FakeClient:
            def __init__(self, credential=None):
                self.credential = credential

            def resources(self, query_req):
                sub_id = query_req.subscriptions[0]
                return types.SimpleNamespace(data=data_by_sub[sub_id])

        module.QueryRequest = FakeQueryRequest
        module.ResourceGraphClient = FakeClient
        module.DefaultAzureCredential = lambda: types.SimpleNamespace(
            get_token=lambda _scope: object()
        )

        result = module.get_deployed_resource_types(["sub1", "sub2"])

        self.assertEqual(
            result,
            {
                "microsoft.web/sites",
                "microsoft.keyvault/vaults",
                "microsoft.storage/storageaccounts",
            },
        )


if __name__ == "__main__":
    unittest.main()
