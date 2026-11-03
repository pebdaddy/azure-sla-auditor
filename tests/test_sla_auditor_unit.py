"""Unit tests for sla_auditor logic using offline Azure SDK stubs."""

import datetime as dt
import importlib
import json
import logging
import os
import sys
import tempfile
import types
import unittest
from unittest import mock


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

    def test_resource_availability_uses_clipped_interval_union(self):
        module = self._import_module()
        start = dt.datetime(2025, 10, 1)
        cases = [
            ("both_half_down", [("app1", 0, 30), ("app2", 0, 30)], 50.0),
            ("one_half_down", [("app1", 0, 30)], 75.0),
            ("overlap_and_duplicate", [
                ("app1", 0, 20), ("app1", 10, 30), ("app1", 0, 20),
            ], 75.0),
            ("crossing_boundaries", [
                ("app1", -15, 15), ("app1", 45, 75),
                ("app1", -30, -15), ("app1", 75, 90),
            ], 75.0),
            ("full_period", [("app1", -10, 70), ("app2", 0, 60)], 0.0),
            ("no_events", [], 100.0),
            ("disjoint_intervals", [("app1", 0, 10), ("app1", 20, 30)], 83.3333333333),
            ("out_of_scope", [("app3", 0, 60)], 100.0),
            ("case_insensitive", [("APP1", 0, 30)], 75.0),
            ("zero_length", [("app1", 10, 10)], 100.0),
        ]
        for name, intervals, expected in cases:
            with self.subTest(name=name):
                windows = [
                    module.DowntimeWindow(
                        resource_id=resource_id,
                        start_utc=start + dt.timedelta(minutes=begin),
                        end_utc=start + dt.timedelta(minutes=finish),
                        source="ServiceHealth" if index % 2 == 0 else "ResourceHealth",
                    )
                    for index, (resource_id, begin, finish) in enumerate(intervals)
                ]
                observed = module.compute_observed_sla(windows, 60, ["app1", "app2"], start)
                self.assertAlmostEqual(observed, expected, places=6)

    def test_resource_health_id_suffix_and_utc_offsets(self):
        module = self._import_module()
        resource_id = "/subscriptions/x/resourceGroups/RG/providers/Microsoft.Web/sites/app1"
        window = module.DowntimeWindow(
            resource_id=resource_id.upper() + "/providers/Microsoft.ResourceHealth/availabilityStatuses/current",
            start_utc=dt.datetime.fromisoformat("2025-10-01T01:00:00+01:00"),
            end_utc=dt.datetime.fromisoformat("2025-10-01T01:30:00+01:00"),
            source="ResourceHealth",
        )
        observed = module.compute_observed_sla(
            [window], 60, [resource_id, resource_id.lower()], dt.datetime(2025, 10, 1)
        )
        self.assertEqual(observed, 50.0)

    def test_invalid_downtime_inputs_raise(self):
        module = self._import_module()
        for duration in [0, -60, float("inf"), float("nan")]:
            with self.subTest(duration=duration), self.assertRaises(ValueError):
                module.compute_observed_sla([], duration)
        window = module.DowntimeWindow(
            "app1", dt.datetime(2025, 10, 1, 1), dt.datetime(2025, 10, 1), "ResourceHealth"
        )
        with self.assertRaisesRegex(ValueError, "ends before it starts"):
            module.compute_observed_sla([window], 60, ["app1"], dt.datetime(2025, 10, 1))

    def test_health_queries_include_returned_pre_period_events(self):
        module = self._import_module()
        start = dt.datetime(2025, 10, 1)
        end = start + dt.timedelta(hours=1)
        resource_id = "/subscriptions/x/resourceGroups/rg/providers/microsoft.web/sites/app1"
        module.arg.resources = mock.Mock(side_effect=[
            types.SimpleNamespace(data=[{
                "id": "incident1",
                "impactStart": "2025-09-30T23:45:00Z",
                "impactMitigation": "2025-10-01T00:15:00Z",
                "incidentTitle": "Service incident",
                "status": "Resolved",
            }]),
            types.SimpleNamespace(data=[{"targetResourceId": resource_id}]),
            types.SimpleNamespace(data=[{
                "id": resource_id + "/providers/Microsoft.ResourceHealth/availabilityStatuses/current",
                "occur": "2025-09-30T23:45:00Z",
            }]),
        ])
        sh, incidents = module.service_health_downtime(["sub1"], start, end)
        rh = module.resource_health_downtime(["sub1"], start, end)
        requests = [call.args[0] for call in module.arg.resources.call_args_list]
        self.assertTrue(all(request.subscriptions == ["sub1"] for request in requests))
        self.assertIn("impactStart < datetime", requests[0].query)
        self.assertIn("isnull(impactMitigation) or impactMitigation >", requests[0].query)
        self.assertNotIn("impactStart >=", requests[0].query)
        self.assertIn("occur < datetime", requests[2].query)
        self.assertNotIn("occur >=", requests[2].query)
        self.assertEqual(len(incidents), 1)
        self.assertEqual(module.compute_observed_sla(sh, 60, [resource_id], start), 75.0)
        self.assertEqual(module.compute_observed_sla(rh, 60, [resource_id], start), 0.0)

    def test_query_resources_preserves_subscription_type_and_tag_scope(self):
        module = self._import_module()
        module.arg.resources = mock.Mock(return_value=types.SimpleNamespace(data=[{"id": "app1"}]))
        result = module.query_resources(["sub1", "sub2"], "Microsoft.Web/Sites", {"Env": "prod's"})
        request = module.arg.resources.call_args.args[0]
        self.assertEqual(request.subscriptions, ["sub1", "sub2"])
        self.assertIn("tolower(type) == 'microsoft.web/sites'", request.query)
        self.assertIn("tags['Env'] =~ 'prod''s'", request.query)
        self.assertEqual(result["id"].tolist(), ["app1"])

    def test_run_preserves_exports_scope_and_resource_weighted_rollup(self):
        module = self._import_module()
        start, end = module.month_bounds("2025-10-01")
        halfway = start + (end - start) / 2
        ids = [
            "/subscriptions/sub1/resourceGroups/RG/providers/Microsoft.Web/sites/app1",
            "/subscriptions/sub1/resourceGroups/RG/providers/Microsoft.Web/sites/app2",
            "/subscriptions/sub1/resourceGroups/RG/providers/Microsoft.Web/sites/app1/slots/slot1",
        ]
        sh = [module.DowntimeWindow(ids[0].lower(), start - dt.timedelta(hours=1), halfway, "ServiceHealth")]
        rh = [
            module.DowntimeWindow(ids[0].upper(), start, halfway, "ResourceHealth"),
            module.DowntimeWindow(
                ids[2].upper() + "/providers/Microsoft.ResourceHealth/availabilityStatuses/current",
                start, end, "ResourceHealth",
            ),
        ]
        incidents = [{
            "impacted_resources": [ids[0].upper() + "/"],
            "is_platform_wide_incident": False,
            "tracking_id": "incident1",
        }]
        credential = types.SimpleNamespace(get_token=mock.Mock(return_value=object()))
        with tempfile.TemporaryDirectory() as tmp_dir:
            targets_path = os.path.join(tmp_dir, "targets.yaml")
            with open(targets_path, "w", encoding="utf-8") as f:
                f.write(
                    "microsoft.web/sites:\n  azure_service_name: Example Service\n  target_pct: 99.9\n"
                    "microsoft.web/sites/slots:\n  azure_service_name: Example Service\n  target_pct: 99.9\n"
                    "microsoft.keyvault/vaults:\n  azure_service_name: Key Vault\n  target_pct: 99.99\n"
                )
            config = {
                "subscriptions": ["sub1", "", "sub2"],
                "resource_tag_filter": {"Env": "Production"},
                "resource_sub_types": [
                    " Microsoft.Web/Sites ", "microsoft.web/sites/slots",
                    "microsoft.keyvault/vaults", "microsoft.storage/storageaccounts",
                ],
                "sla_targets__master_file_path": targets_path,
                "month_start": "2025-10-01",
                "evidence_out_dir": tmp_dir,
            }
            with mock.patch.object(module, "CONFIG", config), \
                    mock.patch.object(module, "cred", credential), \
                    mock.patch.object(module, "query_resources", side_effect=[
                        module.pd.DataFrame({"id": ids[:2]}),
                        module.pd.DataFrame({"id": ids[2:]}),
                        module.pd.DataFrame(),
                    ]) as inventory, \
                    mock.patch.object(module, "service_health_downtime", return_value=(sh, incidents)) as service_health, \
                    mock.patch.object(module, "resource_health_downtime", return_value=rh) as resource_health, \
                    mock.patch.object(module.logger, "warning") as warning:
                module.run()
            credential.get_token.assert_called_once_with("https://management.azure.com/.default")
            self.assertEqual(
                inventory.call_args_list,
                [
                    mock.call(["sub1", "sub2"], resource_type, {"Env": "Production"})
                    for resource_type in ["microsoft.web/sites", "microsoft.web/sites/slots", "microsoft.keyvault/vaults"]
                ],
            )
            service_health.assert_called_once_with(["sub1", "sub2"], start, end)
            resource_health.assert_called_once_with(["sub1", "sub2"], start, end)
            self.assertFalse(any("monitoring scope" in str(call) for call in warning.call_args_list))
            detailed = module.pd.read_csv(os.path.join(tmp_dir, "sla_by_resource_sub_type.csv"))
            rolled = module.pd.read_csv(os.path.join(tmp_dir, "sla_by_service.csv"))
            service_columns = [
                "azure_service_name", "resource_count", "observed_sla_pct",
                "target_sla_pct", "gap_pct", "month", "measurement_method",
            ]
            self.assertEqual(list(rolled.columns), service_columns)
            self.assertEqual(list(detailed.columns), [service_columns[0], "resource_type"] + service_columns[1:])
            self.assertEqual(detailed["resource_count"].tolist(), [2, 1])
            self.assertEqual(detailed["observed_sla_pct"].tolist(), [75.0, 0.0])
            self.assertEqual(rolled["resource_count"].tolist(), [3])
            self.assertEqual(rolled["observed_sla_pct"].tolist(), [50.0])
            self.assertEqual(rolled["target_sla_pct"].tolist(), [99.9])
            self.assertEqual(rolled["gap_pct"].tolist(), [-49.9])
            self.assertEqual(rolled["measurement_method"].tolist(), ["Service/Resource Health"])
            for filename, expected in [
                ("service_health_windows.json", [w.__dict__ for w in sh]),
                ("resource_health_windows.json", [w.__dict__ for w in rh]),
                ("all_service_health_incidents.json", incidents),
            ]:
                with self.subTest(filename=filename), open(os.path.join(tmp_dir, filename), encoding="utf-8") as f:
                    self.assertEqual(json.load(f), json.loads(json.dumps(expected, default=str)))

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
