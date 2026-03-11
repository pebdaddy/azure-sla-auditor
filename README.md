# Azure SLA Auditor

A Python-based, customer-operated tool for analyzing Azure service availability signals and producing indicative availability evidence to support internal operational resilience and regulatory reporting (for example, FINMA and SOC 2).

**Created by:** Eric Peberdy (Microsoft)
**Status:** Personal, open-source project. Not an official Microsoft product, service, or engineering-supported solution.

## Important Notice
This repository contains customer-side tooling intended to help organizations analyze Azure availability telemetry for their own internal purposes.

This tool does **not**:
- Represent an official Microsoft SLA calculation.
- Confirm or assert SLA breaches.
- Determine service credit eligibility.
- Replace Microsoft's contractual SLA assessment process.
- Act as a Microsoft-supported monitoring or reporting service.

Official SLA determinations and service credits are assessed only through Microsoft's formal service credit claim process, based on the Online Services SLA.

## Overview
The Azure SLA Auditor calculates customer-observed availability indicators for Azure services using:
- Azure Service Health (platform-level incidents)
- Azure Resource Health (resource-specific availability events)

The output is designed to support:
- Internal availability analysis
- Audit evidence collection
- Operational resilience reporting
- Regulatory discussions (for example, FINMA supervisory reviews)

Key distinction: Results produced by this tool are indicative and informational. They are not authoritative SLA measurements.

## Key Capabilities
- **Indicative Availability Analysis**: Calculates customer-observed availability percentages based on Azure health telemetry.
- **Multi-Service Coverage**: Supports multiple Azure services (for example, App Service, Logic Apps, Application Gateway).
- **Audit Evidence Generation**: Produces timestamped CSV and JSON artifacts suitable for internal audit review.
- **Platform-Wide Incident Visibility**: Flags global Azure incidents (for example, Azure Front Door, Entra ID) that may not map directly to individual resources.
- **Scope Transparency**: Explicitly identifies incidents affecting services outside the configured monitoring scope.

## Why Service Health and Resource Health?
Regulated customers are often required to distinguish between:
- Cloud platform availability (Microsoft responsibility)
- Application or configuration issues (customer responsibility)

This tool intentionally focuses on platform health signals published by Azure to support that distinction. It does not attempt to assess application correctness, capacity planning, or customer architecture decisions.

## Installation
### Prerequisites
- Python 3.8 or higher
- Azure subscription(s) with appropriate permissions
- Azure authentication (Azure CLI, Managed Identity, or Service Principal)

### Setup
1. Clone or download this repository.
2. Create a virtual environment (recommended):

```bash
python -m venv .venv
source .venv/Scripts/activate  # Windows
# source .venv/bin/activate    # Linux/macOS
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Authenticate to Azure (for example):

```bash
az login
```

## Configuration
### 1. Create `config.yaml`

```bash
cp config.example.yaml config.yaml
```

Configure:
- Azure subscription IDs
- Audit month start date
- Optional resource tag filters
- Resource types to include

### 2. Discover deployed resource types

```bash
python get_resources.py
```

Copy selected types into `resource_sub_types` in `config.yaml`.

This explicit selection is a deliberate control to support transparency and audit traceability.

### 3. Required Azure Permissions
The tool requires Reader access to:
- Target Azure subscriptions
- Azure Resource Graph

## Usage
Run the monthly analysis:

```bash
python sla_auditor.py
```

If your service footprint changes, re-run resource discovery and update the configuration.

## Testing
The repository's current test suite uses Python's built-in `unittest` framework. Run all unit tests with:

```bash
python -m unittest -v tests.test_sla_auditor_unit tests.test_get_resources_unit
```

## Output Artifacts
All outputs are written to `./evidence`.

- `sla_by_service.csv`
  Customer-observed availability indicators per Azure service (published SLA rollup).
- `sla_by_resource_sub_type.csv`
  Availability indicators by ARM resource sub-type.
- `service_health_windows.json`
  Platform-wide Azure Service Health incidents.
- `resource_health_windows.json`
  Resource-level availability events.
- `all_service_health_incidents.json`
  Complete incident log, including out-of-scope services.

Note: These artifacts are intended to support customer analysis and audit conversations, not entitlement claims.

## SLA Calculation Methodology (Indicative)

```text
Observed Availability % = (Total Minutes - Downtime Minutes) / Total Minutes * 100
```

- Downtime is derived from Azure-published health events.
- Methodology is documented for transparency.
- Results should be independently validated against Azure Portal > Service Health.

## Regulatory and Audit Use
Before using outputs for regulatory or audit purposes, customers should:
- Validate results against Azure Portal data.
- Review findings with internal risk, compliance, and audit teams.
- Treat outputs as supporting evidence, not authoritative determinations.

Microsoft does not certify, approve, or endorse reports generated by this tool.

## Troubleshooting
### Authentication Errors
- Ensure Azure login is valid: `az account show`
- If needed, authenticate: `az login`
- For service principal auth, set `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, and `AZURE_CLIENT_SECRET`.

### No Resources Found
- Verify subscription IDs in `config.yaml`.
- Confirm Reader access to subscription and Resource Graph.
- Confirm tag filter values in `resource_tag_filter`.
- Confirm selected `resource_sub_types` are deployed.

## Disclaimer and Liability
This tool is provided as-is under the MIT License, without warranties of any kind, express or implied.

This project:
- Is authored by a Microsoft employee in a personal capacity.
- Is not endorsed, supported, or maintained by Microsoft Corporation.
- Does not create any contractual obligations.
- Does not modify or extend Microsoft's SLA commitments.

Use of this tool is entirely at the customer's discretion and risk.

## License
MIT License. See `LICENSE` for details.

## References
- Azure Service Health: https://learn.microsoft.com/azure/service-health/
- Azure Resource Health: https://learn.microsoft.com/azure/service-health/resource-health-overview
- Azure Resource Graph: https://learn.microsoft.com/azure/governance/resource-graph/
- Azure Online Services SLA: https://www.microsoft.com/licensing/docs/view/Service-Level-Agreements-SLA-for-Online-Services
