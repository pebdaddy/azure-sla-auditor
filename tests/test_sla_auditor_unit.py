"""Unit tests for sla_auditor logic using offline Azure SDK stubs."""

import datetime as dt
import importlib
import logging
import os
import sys
import tempfile
import types
import unittest


def import_sla_auditor_with_stubs(repo_root: str, working_dir: str):
    """Import sla_auditor with Azure SDK modules stubbed for offline unit tests."""
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

    previous_cwd = os.getcwd()
    os.chdir(working_dir)
    try:
        if "sla_auditor" in sys.modules:
            del sys.modules["sla_auditor"]
        return importlib.import_module("sla_auditor")
    finally:
        os.chdir(previous_cwd)


class TestSlaAuditorUnit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        logging.disable(logging.CRITICAL)

    @classmethod
    def tearDownClass(cls):
        logging.disable(logging.NOTSET)

    def _import_module(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = os.path.join(tmp_dir, "config.yaml")
            with open(config_path, "w", encoding="utf-8") as f:
                f.write("subscriptions: ['test-subscription']\n")

            module = import_sla_auditor_with_stubs(self.repo_root, tmp_dir)
            return module

    def test_kql_escape_literal_escapes_single_quotes(self):
        module = self._import_module()
        self.assertEqual(module.kql_escape_literal("a'b"), "a''b")

    def test_month_bounds_returns_next_month_start(self):
        module = self._import_module()
        start, end = module.month_bounds("2025-10-01")

        self.assertEqual(start, dt.datetime(2025, 10, 1))
        self.assertEqual(end, dt.datetime(2025, 11, 1))

    def test_compute_observed_sla(self):
        module = self._import_module()
        windows = [
            module.DowntimeWindow(
                resource_id="/subscriptions/x/resourceGroups/rg/providers/microsoft.web/sites/app1",
                start_utc=dt.datetime(2025, 10, 1, 0, 0, 0),
                end_utc=dt.datetime(2025, 10, 1, 1, 0, 0),
                source="ResourceHealth",
            )
        ]

        observed = module.compute_observed_sla(windows, total_minutes=60 * 24)  # one day
        self.assertAlmostEqual(observed, 95.8333333333, places=6)

    def test_build_published_sla_rollup_groups_rows(self):
        module = self._import_module()
        df = module.pd.DataFrame(
            [
                {
                    "azure_service_name": "Azure Monitor",
                    "resource_type": "microsoft.insights/components",
                    "resource_count": 2,
                    "observed_sla_pct": 100.0,
                    "target_sla_pct": 99.9,
                    "gap_pct": 0.1,
                    "month": "2025-10-01",
                    "measurement_method": "Service/Resource Health",
                },
                {
                    "azure_service_name": "Azure Monitor",
                    "resource_type": "microsoft.insights/workbooks",
                    "resource_count": 4,
                    "observed_sla_pct": 99.95,
                    "target_sla_pct": 99.9,
                    "gap_pct": 0.05,
                    "month": "2025-10-01",
                    "measurement_method": "Service/Resource Health",
                },
                {
                    "azure_service_name": "Key Vault",
                    "resource_type": "microsoft.keyvault/vaults",
                    "resource_count": 1,
                    "observed_sla_pct": 100.0,
                    "target_sla_pct": 99.99,
                    "gap_pct": 0.01,
                    "month": "2025-10-01",
                    "measurement_method": "Service/Resource Health",
                },
            ]
        )

        rolled = module.build_published_sla_rollup(df)

        self.assertEqual(len(rolled), 2)

        monitor_row = rolled[rolled["azure_service_name"] == "Azure Monitor"].iloc[0]
        self.assertEqual(int(monitor_row["resource_count"]), 6)
        self.assertAlmostEqual(float(monitor_row["observed_sla_pct"]), 99.96667, places=5)
        self.assertAlmostEqual(float(monitor_row["target_sla_pct"]), 99.9, places=5)

    def test_order_output_columns_for_service_csv(self):
        module = self._import_module()
        df = module.pd.DataFrame(
            [
                {
                    "measurement_method": "Service/Resource Health",
                    "gap_pct": 0.1,
                    "resource_count": 2,
                    "month": "2025-10-01",
                    "azure_service_name": "Azure Monitor",
                    "target_sla_pct": 99.9,
                    "observed_sla_pct": 100.0,
                }
            ]
        )

        ordered = module.order_output_columns(
            df,
            [
                "azure_service_name",
                "resource_count",
                "observed_sla_pct",
                "target_sla_pct",
                "gap_pct",
                "month",
                "measurement_method",
            ],
        )

        self.assertEqual(
            list(ordered.columns),
            [
                "azure_service_name",
                "resource_count",
                "observed_sla_pct",
                "target_sla_pct",
                "gap_pct",
                "month",
                "measurement_method",
            ],
        )

    def test_order_output_columns_for_resource_sub_type_csv(self):
        module = self._import_module()
        df = module.pd.DataFrame(
            [
                {
                    "measurement_method": "Service/Resource Health",
                    "gap_pct": 0.1,
                    "resource_count": 2,
                    "month": "2025-10-01",
                    "resource_type": "microsoft.insights/components",
                    "azure_service_name": "Azure Monitor",
                    "target_sla_pct": 99.9,
                    "observed_sla_pct": 100.0,
                }
            ]
        )

        ordered = module.order_output_columns(
            df,
            [
                "azure_service_name",
                "resource_type",
                "resource_count",
                "observed_sla_pct",
                "target_sla_pct",
                "gap_pct",
                "month",
                "measurement_method",
            ],
        )

        self.assertEqual(
            list(ordered.columns),
            [
                "azure_service_name",
                "resource_type",
                "resource_count",
                "observed_sla_pct",
                "target_sla_pct",
                "gap_pct",
                "month",
                "measurement_method",
            ],
        )

    def test_load_targets_catalog_uses_key_as_resource_type(self):
        module = self._import_module()

        with tempfile.TemporaryDirectory() as tmp_dir:
            targets_path = os.path.join(tmp_dir, "targets.yaml")
            with open(targets_path, "w", encoding="utf-8") as f:
                f.write(
                    "microsoft.web/sites:\n"
                    "  azure_service_name: App Service\n"
                    "  target_pct: 99.95\n"
                )

            catalog = module.load_targets_catalog(targets_path)

        self.assertIn("microsoft.web/sites", catalog)
        self.assertEqual(catalog["microsoft.web/sites"]["resource_type"], "microsoft.web/sites")
        self.assertEqual(catalog["microsoft.web/sites"]["target_id"], "microsoft.web/sites")
        self.assertEqual(catalog["microsoft.web/sites"]["azure_service_name"], "App Service")
        self.assertEqual(catalog["microsoft.web/sites"]["target_pct"], 99.95)

    def test_resolve_active_targets_by_resource_type_direct_key_match(self):
        module = self._import_module()

        with tempfile.TemporaryDirectory() as tmp_dir:
            targets_path = os.path.join(tmp_dir, "targets.yaml")
            with open(targets_path, "w", encoding="utf-8") as f:
                f.write(
                    "microsoft.web/sites:\n"
                    "  azure_service_name: App Service\n"
                    "  target_pct: 99.95\n"
                    "microsoft.keyvault/vaults:\n"
                    "  azure_service_name: Key Vault\n"
                    "  target_pct: 99.99\n"
                )

            config = {
                "resource_sub_types": [
                    "microsoft.web/sites",
                    "microsoft.storage/storageaccounts",  # unmatched on purpose
                ],
                "sla_targets__master_file_path": targets_path,
            }

            active_targets = module.resolve_active_targets_by_resource_type(config)

        self.assertEqual(set(active_targets.keys()), {"microsoft.web/sites"})


if __name__ == "__main__":
    unittest.main()
