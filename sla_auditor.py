
# ============================================================================
# SLA Calculator for Azure Services
# ============================================================================
# Purpose: Compute observed monthly SLA per Azure service and produce evidence
#          for auditable compliance reporting
# Author: Eric Peberdy (SR CLOUD SOLUTION ARCHITECT, Zurich)
#
# OVERVIEW
# --------
# This script measures Azure service availability using Azure's platform
# health data and generates monthly SLA evidence for regulatory reporting.
#
# KEY CONCEPTS
# ------------
#
# 1. SERVICE HEALTH (Platform-Wide Incidents)
#    - Microsoft's public announcements of Azure service outages/degradations
#    - Affects multiple resources across regions (e.g., "Logic Apps unavailable in West Europe")
#    - Tracked via Azure Service Health dashboard
#    - Source: Azure Resource Graph - ServiceHealthResources table
#    - Example: Regional datacenter issue affecting all Logic Apps in a region
#    - Captures ALL ServiceIssue events for comprehensive incident tracking
#    - Detects platform-wide incidents via keyword matching (AFD, Portal, Entra, etc.)
#
# 2. RESOURCE HEALTH (Individual Resource Availability)
#    - Availability status of each individual Azure resource
#    - Tracks Unavailable/Degraded/Available states per resource
#    - Source: Azure Resource Graph - HealthResources table
#    - Example: One specific Logic App becomes unavailable due to backend issue
#
# SLA CALCULATION METHODOLOGY
# ----------------------------
#
# PRIMARY (SLA Calculation):
#   Service Health + Resource Health = Platform Availability
#   - This follows Azure's published SLA measurement approach
#   - Isolates Azure platform responsibility from application issues
#   - Used for SLA credits and contractual compliance
#   - Recommended for regulatory reporting (FINMA, audit evidence)
#
# WHY THIS DISTINCTION MATTERS
# -----------------------------
# For regulatory compliance (FINMA, audits), you need to report on Azure's
# platform reliability, NOT your application's code quality. Service/Resource
# Health isolates platform issues from application bugs, configuration errors,
# or capacity limits.
#
# Example:
#   - Logic App workflow fails due to bad HTTP 500 from external API
#     → Workload metric shows failure (RunsFailed increases)
#     → Service/Resource Health shows no platform issue
#     → SLA: 100% (Azure platform was available, your app logic failed)
#
#   - Azure Logic App service has regional outage
#     → Workload metric shows failure (runs don't complete)
#     → Service Health shows incident, Resource Health shows Unavailable
#     → SLA: <100% (Azure platform issue, eligible for SLA credit)
#
# PLATFORM-WIDE INCIDENT DETECTION
# ----------------------------------
# This tool detects a critical category: global Azure infrastructure failures that
# don't map to specific customer resources. These incidents appear in Service
# Health but have zero impacted resources listed because they affect Microsoft's
# own infrastructure (e.g., Azure Front Door platform failure affecting Portal,
# M365, Entra ID, and cascading to customer services).
#
# Detection Keywords:
#   - "front door", "frontdoor", "afd"
#   - "azure portal", "entra", "azure ad", "authentication"
#   - "global", "platform", "infrastructure"
#   - "configuration change", "metadata"
#
# When detected, these incidents are:
#   1. Flagged with prominent warnings in console output
#   2. Saved to all_service_health_incidents.json with is_platform_wide_incident flag
#   3. Recommended for manual impact assessment
#
# USAGE
# -----
# 1. Configure config.yaml with subscriptions and services to monitor
# 2. Run monthly: python sla_auditor.py
# 3. Outputs:
#    - sla_by_resource_sub_type.csv: SLA report by resource sub-type (Service/Resource Health)
#    - sla_by_service.csv: Published SLA rollup report (one row per service/SLA)
#    - service_health_windows.json: Platform incidents affecting monitored resources
#    - resource_health_windows.json: Resource availability events
#    - all_service_health_incidents.json: Comprehensive incident log with all platform events
#
# Console Output:
#    - Summary statistics: total incidents, platform-wide count, unmonitored count
#    - Platform-wide incidents flagged with portal search instructions
#    - Unmonitored incidents flagged for config.yaml updates
#    - All detailed incident data available in all_service_health_incidents.json
#
# DEPENDENCIES
# ------------
# - azure-mgmt-resourcegraph: Query Service Health, Resource Health, inventory
# - azure-identity: Azure authentication (DefaultAzureCredential)
# - pandas, pyyaml: Data processing and configuration
#
# REGULATORY COMPLIANCE
# ---------------------
# This approach follows Azure's published SLA methodology and provides
# auditable evidence trail suitable for:
# - FINMA (Swiss Financial Market Supervisory Authority) reporting
# - SOC 2 compliance evidence
# - Financial services regulatory audits
# - Operational resilience reporting
#
# This tool captures all platform incidents, including those that don't directly
# map to customer resources, providing comprehensive evidence of Azure platform
# availability for regulatory compliance and audit purposes.
#
# ============================================================================

import os, json, datetime as dt
import logging
import re
from dataclasses import dataclass
from typing import List, Dict, Tuple, Any
import pandas as pd
import yaml

from azure.identity import DefaultAzureCredential
from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest

logger = logging.getLogger(__name__)


def ensure_azure_authentication(credential) -> None:
    """Fail fast with a clear message when no Azure auth context is available."""
    try:
        credential.get_token("https://management.azure.com/.default")
    except Exception as exc:
        logger.error("Azure authentication failed. Run 'az login' and try again.")
        logger.error("If using a service principal, set AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_CLIENT_SECRET.")
        logger.debug("Underlying authentication error: %s", exc)
        raise SystemExit(1)

# -----------------------------
# Config (service targets, metrics strategy, scopes)
# -----------------------------
def load_config(config_file: str = "config.yaml") -> dict:
    """Load configuration from YAML file."""
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    # Set default evidence_out_dir if not in config
    if "evidence_out_dir" not in config:
        config["evidence_out_dir"] = "./evidence"

    # Set default month_start if not in config
    if "month_start" not in config or not config["month_start"]:
        config["month_start"] = dt.date.today().replace(day=1).isoformat()

    return config


def load_targets_catalog(targets_file: str) -> Dict[str, dict]:
    """Load SLA target catalog where YAML keys ARE resource types (e.g., 'microsoft.web/sites').
    
    Each entry is normalized to: {azure_service_name, resource_type, target_pct}
    Returns: {target_id → {azure_service_name, resource_type, target_pct}}
    """
    with open(targets_file, "r", encoding="utf-8") as f:
        raw_targets = yaml.safe_load(f) or {}

    if not isinstance(raw_targets, dict):
        raise ValueError(f"Invalid targets catalog format in {targets_file}: expected YAML object")

    normalized_targets = {}
    for resource_type_key, value in raw_targets.items():
        if not isinstance(value, dict):
            continue
        
        # Key IS the resource_type (official Azure format, e.g., microsoft.web/sites)
        resource_type = resource_type_key.strip().lower()
        azure_service_name = value.get("azure_service_name", resource_type)
        target_pct = value.get("target_pct")
        
        if target_pct is None:
            continue
        
        # Use resource_type as target_id for exact matching in resolve_active_targets_by_resource_type
        normalized_targets[resource_type] = {
            "target_id": resource_type,
            "resource_type": resource_type,
            "azure_service_name": azure_service_name,
            "target_pct": target_pct
        }
    
    return normalized_targets


def namespace_from_resource_type(resource_type: str) -> str:
    """Normalize a resource type to provider namespace, e.g. Microsoft.Web/sites -> microsoft.web."""
    if not isinstance(resource_type, str) or not resource_type.strip():
        return ""
    return resource_type.split("/", 1)[0].strip().lower()


def load_resource_property_mappings(config: dict) -> Dict[str, dict]:
    """Load optional property mappings for variant-level resource filtering."""
    mappings_file = config.get("resource_property_mappings_path")
    if not mappings_file:
        targets_file = config.get("sla_targets__master_file_path")
        if targets_file:
            mappings_file = os.path.join(os.path.dirname(targets_file), "resource_property_mappings.yaml")

    if not mappings_file or not os.path.exists(mappings_file):
        return {}

    with open(mappings_file, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    if not isinstance(raw, dict):
        raise ValueError(f"Invalid resource property mappings format in {mappings_file}: expected YAML object")
    return raw


def resolve_active_targets_by_resource_type(config: dict) -> Dict[str, dict]:
    """Resolve resource sub-types from config and return matching targets from catalog.
    
    Catalog keys ARE resource_types (e.g., 'microsoft.network/loadbalancers').
    Config resource_sub_types are matched directly against catalog keys.
    """
    resource_sub_types = config.get("resource_sub_types")
    targets_file = config.get("sla_targets__master_file_path")

    if not targets_file:
        raise ValueError("sla_targets__master_file_path is missing in config.yaml")

    if resource_sub_types is None:
        raise ValueError("resource_sub_types is missing in config.yaml")

    if not isinstance(resource_sub_types, list) or not all(isinstance(r, str) for r in resource_sub_types):
        raise ValueError("resource_sub_types must be a list of strings in config.yaml")

    selected_types = [r.strip().lower() for r in resource_sub_types if r and r.strip()]
    if not selected_types:
        raise ValueError("resource_sub_types in config.yaml is empty")

    catalog = load_targets_catalog(targets_file)

    # Catalog keys ARE resource_types; direct lookup
    active_targets: Dict[str, dict] = {}
    unmatched = []
    
    for selected_type in selected_types:
        if selected_type in catalog:
            active_targets[selected_type] = catalog[selected_type]
        else:
            unmatched.append(selected_type)

    # Warn about unmatched resource sub-types
    if unmatched:
        logger.warning("%d resource sub-type(s) not found in %s:", len(unmatched), targets_file)
        for t in unmatched:
            logger.warning("  - %s", t)
        logger.warning("These will be skipped.")

    if not active_targets:
        raise ValueError(
            f"None of the requested resource sub-types match any targets in {targets_file}. "
            f"Check resource_sub_types spelling or update {targets_file}."
        )

    return active_targets


def infer_variant_name(target: dict) -> str:
    """Infer variant name from target metadata (description fallback)."""
    if isinstance(target.get("variant_name"), str) and target["variant_name"].strip():
        return target["variant_name"].strip()
    desc = str(target.get("description", "")).strip()
    if " - " in desc:
        return desc.split(" - ", 1)[1].strip()
    return ""


def normalize_scalar(value: Any) -> Any:
    if isinstance(value, str):
        return value.strip()
    return value


def get_path_value(obj: Any, path: str) -> Any:
    """Resolve dot path with optional [index] selectors from nested dict/list objects."""
    if obj is None or not path:
        return None

    current = obj
    for part in path.split("."):
        match = re.fullmatch(r"([^\[]+)(?:\[(\d+)\])?", part)
        if not match:
            return None

        key = match.group(1)
        idx = match.group(2)

        if isinstance(current, dict):
            current = current.get(key)
        else:
            return None

        if idx is not None:
            if not isinstance(current, list):
                return None
            index = int(idx)
            if index < 0 or index >= len(current):
                return None
            current = current[index]

    return current


def field_value_for_rule(row: dict, field: str, mapping: dict) -> Any:
    """Resolve a rule field to an actual value from row payload."""
    field = field.strip()

    # Aliases frequently used by rule definitions.
    if field == "tier":
        tier_path = mapping.get("tier_path")
        if tier_path:
            return get_path_value(row, tier_path)
    if field == "capacity":
        cap_path = mapping.get("instance_count_path")
        if cap_path:
            return get_path_value(row, cap_path)
    if field == "zones":
        zone_path = mapping.get("zone_path") or "zones"
        return get_path_value(row, zone_path)
    if field == "locations":
        return get_path_value(row, "properties.locations")
    if field == "edition":
        return get_path_value(row, "properties.edition")
    if field == "availabilitySet":
        return get_path_value(row, "properties.availabilitySet")
    if field == "storageProfile":
        return get_path_value(row, "properties.storageProfile")

    # Try direct path and common prefixes.
    candidates = [field]
    if not field.startswith("properties."):
        candidates.append(f"properties.{field}")
    if not field.startswith("sku."):
        candidates.append(f"sku.{field}")

    for candidate in candidates:
        value = get_path_value(row, candidate)
        if value is not None:
            return value
    return None


def evaluate_condition(value: Any, condition: str) -> bool:
    """Evaluate a simple textual condition against a value."""
    if condition is None:
        return True

    cond = str(condition).strip()
    cond_l = cond.lower()
    val = normalize_scalar(value)
    val_s = "" if val is None else str(val)
    val_l = val_s.lower()

    if cond_l == "not-null":
        return val is not None
    if cond_l == "null or empty":
        if val is None:
            return True
        if isinstance(val, (list, dict, str)):
            return len(val) == 0
        return False
    if cond_l == "null or disabled":
        return val is None or val_l in {"disabled", "none", "null", ""}

    if cond_l.startswith("length >"):
        try:
            n = int(cond_l.split(">", 1)[1].strip())
            return hasattr(val, "__len__") and len(val) > n
        except Exception:
            return False
    if cond_l.startswith("length =="):
        try:
            n = int(cond_l.split("==", 1)[1].strip())
            return hasattr(val, "__len__") and len(val) == n
        except Exception:
            return False

    if cond_l.startswith(">"):
        try:
            return float(val) > float(cond_l[1:].strip())
        except Exception:
            return False
    if cond_l.startswith("=="):
        right = cond_l[2:].strip()
        try:
            return float(val) == float(right)
        except Exception:
            return val_l == right

    if cond_l.startswith("contains "):
        expr = cond_l[len("contains "):].strip()
        if " and not " in expr:
            pos, neg = expr.split(" and not ", 1)
            return pos.strip() in val_l and neg.strip() not in val_l
        if " or " in expr:
            parts = [p.strip() for p in expr.split(" or ")]
            return any(p and p in val_l for p in parts)
        return expr in val_l

    if cond_l == "not-null and length > 0":
        return val is not None and hasattr(val, "__len__") and len(val) > 0
    if cond_l == "has premium managed disks":
        return "premium" in json.dumps(val, default=str).lower()
    if cond_l == "has standard managed disks":
        return "standard" in json.dumps(val, default=str).lower()

    # Default to case-insensitive equality.
    return val_l == cond_l


def rule_matches_row(row: dict, rule: dict, mapping: dict) -> bool:
    """Check if a single row satisfies all rule requirements."""
    requires = rule.get("requires")
    if not requires:
        return True

    if isinstance(requires, dict):
        requires = [{k: v} for k, v in requires.items()]

    if not isinstance(requires, list):
        return False

    for req in requires:
        if not isinstance(req, dict):
            return False
        for field, cond in req.items():
            value = field_value_for_rule(row, field, mapping)
            if not evaluate_condition(value, cond):
                return False
    return True


def filter_resources_for_target(df: pd.DataFrame, target: dict, mapping: dict) -> Tuple[pd.DataFrame, bool]:
    """Filter resource dataframe for a target variant when a mapping rule exists."""
    if df.empty:
        return df, False

    variant = infer_variant_name(target)
    if not variant:
        return df, False

    rules = mapping.get("matching_rules") or []
    if not isinstance(rules, list):
        return df, False

    selected_rule = None
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        if str(rule.get("variant", "")).strip().lower() == variant.lower():
            selected_rule = rule
            break

    if selected_rule is None:
        return df, False

    matched_indices = []
    for idx, row in df.iterrows():
        row_dict = row.to_dict()
        if rule_matches_row(row_dict, selected_rule, mapping):
            matched_indices.append(idx)

    if not matched_indices:
        return df.iloc[0:0].copy(), True

    return df.loc[matched_indices].copy(), True

CONFIG = load_config()

# -----------------------------
# Helpers
# -----------------------------
@dataclass
class DowntimeWindow:
    resource_id: str
    start_utc: dt.datetime
    end_utc: dt.datetime
    source: str  # "ServiceHealth" or "ResourceHealth"

def month_bounds(month_start_iso: str) -> Tuple[dt.datetime, dt.datetime]:
    start = dt.datetime.fromisoformat(month_start_iso)
    next_month = (start.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return start, next_month


def kql_escape_literal(value: Any) -> str:
    """Escape values used inside single-quoted KQL literals."""
    return str(value).replace("'", "''")

# -----------------------------
# Initialize clients - we use Entra and Azure Resource Graph
# -----------------------------
cred = DefaultAzureCredential()
arg = ResourceGraphClient(credential=cred)

# -----------------------------
# 1) Inventory via ARG
# -----------------------------
def query_resources(subs: List[str], resource_type: str, tag_filter: dict = None) -> pd.DataFrame:
    """
    Query Azure resources by type and optionally filter by tags.

    Args:
        subs: List of subscription IDs
        resource_type: Azure resource type/subtype (e.g., 'microsoft.logic/workflows')
        tag_filter: Optional dict of tag key-value pairs (e.g., {'Environment': 'Production'})
    """
    resource_type_escaped = kql_escape_literal(resource_type.lower())
    kql = f"""
    resources
    | where tolower(type) == '{resource_type_escaped}'
    """

    # Add tag filter if provided
    if tag_filter:
        for tag_key, tag_value in tag_filter.items():
            escaped_key = kql_escape_literal(tag_key)
            escaped_value = kql_escape_literal(tag_value)
            kql += f"\n    | where tags['{escaped_key}'] =~ '{escaped_value}'"

    kql += "\n    | project id, name, resourceGroup, subscriptionId, location, type, kind, zones, tags, sku, properties"

    req = QueryRequest(subscriptions=subs, query=kql)
    result = arg.resources(req)
    return pd.DataFrame(result.data)

# -----------------------------
# 2) Service Health events & impacted resources via ARG (downtime windows) [6](https://learn.microsoft.com/en-us/azure/service-health/resource-graph-samples)[7](https://docs.azure.cn/en-us/service-health/resource-graph-impacted-samples)
# -----------------------------
def service_health_downtime(subs: List[str], start: dt.datetime, end: dt.datetime) -> Tuple[List[DowntimeWindow], List[dict]]:
    # Service issues only; planned maintenance can be handled separately per FINMA BCM disclosures.

    kql = f"""
    ServiceHealthResources
    | where type =~ 'microsoft.resourcehealth/events'
    | extend p = parse_json(properties)
    | extend eventType = tostring(p.EventType), status = tostring(p.Status),
             impactStart = todatetime(p.ImpactStartTime), impactMitigation = todatetime(p.ImpactMitigationTime),
             incidentTitle = tostring(p.Title), incidentSummary = tostring(p.Summary)
    | where eventType == 'ServiceIssue'
    | where impactStart >= datetime({start.isoformat()}) and impactStart < datetime({end.isoformat()})
    | project name, id, impactStart, impactMitigation, status, incidentTitle, incidentSummary
    """

    req = QueryRequest(subscriptions=subs, query=kql)
    rows = arg.resources(req).data

    windows = []
    all_incidents = []  # Track all incidents for comprehensive reporting

    for idx, r in enumerate(rows, 1):
        tracking_id = r["id"]
        title = r.get("incidentTitle", "N/A")
        status = r.get("status", "N/A")

        # Join impacted resources - try multiple query approaches
        # Approach 1: Direct match on parent event ID
        kql_ir = f"""
        ServiceHealthResources
        | where type == 'microsoft.resourcehealth/events/impactedresources'
        | where id startswith '{tracking_id}/impactedResources/'
        | extend p = parse_json(properties)
        | project targetResourceId = tostring(p.targetResourceId), targetResourceType = tostring(p.targetResourceType)
        """
        ir_req = QueryRequest(subscriptions=subs, query=kql_ir)
        ir_rows = arg.resources(ir_req).data

        # Extract unique resource types
        resource_types = set()
        for ir in ir_rows:
            if "targetResourceId" in ir and ir["targetResourceId"]:
                # Extract resource type from resource ID
                parts = ir["targetResourceId"].split('/')
                if len(parts) >= 8:  # Typical Azure resource ID format
                    resource_type = f"{parts[6]}/{parts[7]}"
                    resource_types.add(resource_type)

        # Check for platform infrastructure keywords in title/summary
        platform_keywords = [
            "front door", "frontdoor", "afd",
            "azure portal", "entra", "azure ad", "authentication",
            "global", "platform", "infrastructure",
            "configuration change", "metadata"
        ]

        incident_text = f"{title} {r.get('incidentSummary', '')}".lower()
        matched_keywords = [kw for kw in platform_keywords if kw in incident_text]

        # Store incident details for comprehensive reporting
        incident_detail = {
            "tracking_id": tracking_id,
            "title": title,
            "status": status,
            "summary": r.get("incidentSummary", "N/A"),
            "impact_start": r["impactStart"],
            "impact_mitigation": r.get("impactMitigation"),
            "impacted_resources": [ir["targetResourceId"] for ir in ir_rows],
            "impacted_resource_count": len(ir_rows),
            "affected_resource_types": sorted(list(resource_types)),
            "platform_keywords_detected": matched_keywords,
            "is_platform_wide_incident": len(matched_keywords) > 0 and len(ir_rows) == 0
        }
        all_incidents.append(incident_detail)

        for ir in ir_rows:
            windows.append(DowntimeWindow(
                resource_id=ir["targetResourceId"],
                start_utc=dt.datetime.fromisoformat(r["impactStart"]),
                end_utc=dt.datetime.fromisoformat(r["impactMitigation"]) if r["impactMitigation"] else end,
                source="ServiceHealth"
            ))
    return windows, all_incidents

# -----------------------------
# 3) Resource Health availabilitystatuses (downtime windows) [9](https://learn.microsoft.com/en-us/azure/service-health/resource-health-overview)
# -----------------------------
def resource_health_downtime(subs: List[str], start: dt.datetime, end: dt.datetime) -> List[DowntimeWindow]:
    kql = f"""
    HealthResources
    | where type =~ 'microsoft.resourcehealth/availabilitystatuses'
    | extend p = parse_json(properties)
    | extend status = tostring(p.availabilityState),
             occur = todatetime(p.occuredTime), reason = tostring(p.reasonType)
    | where occur >= datetime({start.isoformat()}) and occur < datetime({end.isoformat()})
    | where status in ('Unavailable','Degraded')
    | project id, name, occur, status
    """
    req = QueryRequest(subscriptions=subs, query=kql)
    rows = arg.resources(req).data
    # Note: end time may require subsequent status “Available/Resolved” event lookup; simplified here.
    windows = []
    for r in rows:
        windows.append(DowntimeWindow(resource_id=r["id"], start_utc=dt.datetime.fromisoformat(r["occur"]),
                                      end_utc=end, source="ResourceHealth"))
    return windows

# -----------------------------
# 4) Compute SLA per service
# -----------------------------
def compute_observed_sla(windows: List[DowntimeWindow], total_minutes: float) -> float:
    downtime = sum([(w.end_utc - w.start_utc).total_seconds()/60.0 for w in windows])
    return 100.0 * (1.0 - downtime / total_minutes)


def build_published_sla_rollup(by_service_df: pd.DataFrame) -> pd.DataFrame:
    """Roll up service rows to one row per published SLA (service + target SLA)."""
    columns = [
        "azure_service_name",
        "target_sla_pct",
        "resource_count",
        "observed_sla_pct",
        "gap_pct",
        "month",
        "measurement_method",
    ]
    if by_service_df.empty:
        return pd.DataFrame(columns=columns)

    grouped_rows = []
    group_keys = ["azure_service_name", "target_sla_pct", "month", "measurement_method"]
    for (service_name, target_sla, month, measurement_method), group in by_service_df.groupby(group_keys, dropna=False):
        total_resources = float(group["resource_count"].sum())
        if total_resources > 0:
            observed_rollup = (group["observed_sla_pct"] * group["resource_count"]).sum() / total_resources
        else:
            observed_rollup = float(group["observed_sla_pct"].mean())

        target_value = float(target_sla)
        grouped_rows.append(
            {
                "azure_service_name": service_name,
                "target_sla_pct": target_value,
                "resource_count": int(total_resources),
                "observed_sla_pct": round(float(observed_rollup), 5),
                "gap_pct": round(float(observed_rollup) - target_value, 5),
                "month": month,
                "measurement_method": measurement_method,
            }
        )

    return pd.DataFrame(grouped_rows, columns=columns).sort_values(
        ["azure_service_name", "target_sla_pct"], ignore_index=True
    )

# -----------------------------
# 6) Main
# -----------------------------
def run():
    os.makedirs(CONFIG["evidence_out_dir"], exist_ok=True)
    ensure_azure_authentication(cred)
    subs = [s for s in CONFIG["subscriptions"] if s]
    start, end = month_bounds(CONFIG["month_start"])
    tag_filter = CONFIG.get("resource_tag_filter")

    active_targets = resolve_active_targets_by_resource_type(CONFIG)

    # Inventory: Query Azure Resource Graph for all configured targets
    logger.info("Querying resources in %d subscription(s)...", len(subs))
    if tag_filter:
        logger.info("Filtering by tags: %s", tag_filter)
    logger.info("Using %d service target(s)", len(active_targets))

    resources_by_target = {}
    for target_id, target_def in active_targets.items():
        resource_type = target_def.get("resource_type", "")
        if not resource_type:
            logger.warning("Missing resource_type for %s, skipping...", target_id)
            continue
        df = query_resources(subs, resource_type, tag_filter)
        resources_by_target[target_id] = df
        if len(df) > 0:
            logger.info("Found %d resources for %s (%s)", len(df), target_id, resource_type)
        else:
            logger.debug("No matching resources for %s (%s)", target_id, resource_type)

    matched_targets = {
        target_id: target_def
        for target_id, target_def in active_targets.items()
        if target_id in resources_by_target and not resources_by_target[target_id].empty
    }
    skipped_target_count = len(active_targets) - len(matched_targets)
    if skipped_target_count > 0:
        logger.info(
            "Skipping %d configured resource sub-type(s) with zero matched resources.",
            skipped_target_count,
        )

    # PRIMARY SLA CALCULATION: Service Health + Resource Health (Azure platform availability)
    # This follows Azure's published SLA measurement approach
    sh, all_service_health_incidents = service_health_downtime(subs, start, end)
    rh = resource_health_downtime(subs, start, end)
    total_minutes = (end - start).total_seconds()/60.0

    # Validate Service Health incidents against monitored resources
    all_monitored_resources = set()
    for df in resources_by_target.values():
        if not df.empty:
            all_monitored_resources.update(df["id"])

    # Check for unmonitored incidents
    unmonitored_incidents = []
    platform_wide_incidents = []

    for incident in all_service_health_incidents:
        impacted_resources = set(incident["impacted_resources"])

        # Track platform-wide incidents (e.g., global AFD failure affecting Azure Portal, M365, etc.)
        if incident.get("is_platform_wide_incident", False):
            platform_wide_incidents.append(incident)
        # Track incidents affecting unmonitored resources
        elif impacted_resources and not impacted_resources.intersection(all_monitored_resources):
            unmonitored_incidents.append(incident)

    # Report platform-wide infrastructure incidents
    if platform_wide_incidents:
        logger.warning("%d platform-wide infrastructure incident(s) detected (see all_service_health_incidents.json for details)", len(platform_wide_incidents))

    if unmonitored_incidents:
        logger.warning("Found %d Service Health incident(s) affecting resources not in your monitoring scope.", len(unmonitored_incidents))
        logger.warning("These incidents may indicate outages for resource sub-types not included in selected config resource_sub_types.")
        for inc in unmonitored_incidents:
            logger.warning("  - Incident %s: %s impacted resources", inc['tracking_id'], inc['impacted_resource_count'])
            # Show sample resource to identify service type
            if inc['impacted_resources']:
                sample = inc['impacted_resources'][0]
                resource_type = '/'.join(sample.split('/')[-3:-1]) if '/' in sample else 'Unknown'
                logger.warning("    Resource type example: %s", resource_type)
        logger.warning("Consider adding relevant resource sub-types to config.yaml")

    # Map downtime windows to targets based on resource IDs
    target_downtime_map = {}
    for target_id, df in resources_by_target.items():
        if df.empty:
            target_downtime_map[target_id] = []
            continue
        resource_ids = set(df["id"])
        target_downtime_map[target_id] = [w for w in sh + rh if w.resource_id in resource_ids]

    # Calculate SLA per service using Service/Resource Health (high fidelity, per-resource-type)
    by_service = []
    for target_id, target_def in matched_targets.items():
        svc_windows = target_downtime_map.get(target_id, [])
        observed = compute_observed_sla(svc_windows, total_minutes)
        by_service.append({
            "azure_service_name": target_def.get("azure_service_name", target_id),
            "resource_type": target_def.get("resource_type", ""),
            "resource_count": len(resources_by_target.get(target_id, pd.DataFrame())),
            "observed_sla_pct": round(observed, 5),
            "target_sla_pct": target_def.get("target_pct"),
            "gap_pct": round(observed - target_def.get("target_pct", 99.9), 5),
            "month": CONFIG["month_start"],
            "measurement_method": "Service/Resource Health"
        })

    # Save evidence (for audit/compliance)
    # PRIMARY: SLA calculated from Service/Resource Health (regulatory reporting)
    by_service_df = pd.DataFrame(by_service)
    by_service_df.to_csv(os.path.join(CONFIG["evidence_out_dir"], "sla_by_resource_sub_type.csv"), index=False)

    # Roll up detailed rows to one row per published SLA target.
    published_sla_df = build_published_sla_rollup(by_service_df)
    published_sla_df.to_csv(os.path.join(CONFIG["evidence_out_dir"], "sla_by_service.csv"), index=False)

    # Platform availability evidence
    with open(os.path.join(CONFIG["evidence_out_dir"], "service_health_windows.json"), "w") as f:
        json.dump([w.__dict__ for w in sh], f, default=str, indent=2)
    with open(os.path.join(CONFIG["evidence_out_dir"], "resource_health_windows.json"), "w") as f:
        json.dump([w.__dict__ for w in rh], f, default=str, indent=2)

    # Save ALL Service Health incidents (including unmonitored services) for comprehensive audit trail
    with open(os.path.join(CONFIG["evidence_out_dir"], "all_service_health_incidents.json"), "w") as f:
        json.dump(all_service_health_incidents, f, default=str, indent=2)

    logger.info("SLA calculation complete for %s", CONFIG['month_start'])
    logger.info("Evidence saved to %s", CONFIG['evidence_out_dir'])
    logger.info("SLA measured using Service/Resource Health (Azure platform availability)")
    logger.info("Published SLA rollup report saved to %s", os.path.join(CONFIG["evidence_out_dir"], "sla_by_service.csv"))
    logger.info("Total Service Health incidents found: %d", len(all_service_health_incidents))
    if platform_wide_incidents:
        logger.warning("Platform-wide incidents: %d of %d (require manual review)", len(platform_wide_incidents), len(all_service_health_incidents))
        logger.warning("Search for tracking IDs in Azure Portal > Service Health > Health History")
    if unmonitored_incidents:
        logger.warning("Unmonitored incidents: %d of %d (see all_service_health_incidents.json)", len(unmonitored_incidents), len(all_service_health_incidents))

if __name__ == "__main__":
    log_level = os.getenv("SLA_AUDITOR_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(levelname)s %(message)s"
    )
    if log_level != "DEBUG":
        logging.getLogger("azure").setLevel(logging.WARNING)
        logging.getLogger("azure.identity").setLevel(logging.WARNING)
    run()
