#!/usr/bin/env python3
"""
Extract and list all unique Azure resource sub-types (full types including namespace and resource kind)
currently deployed in the subscriptions specified in the config YAML file, by querying Azure Resource Graph.

Outputs resource sub-type names (for example, microsoft.web/sites) to the console and
optionally writes them to a file.

Usage:
        python get_resources.py [--config CONFIG] [--output OUTPUT]
        --config: Path to config YAML with 'subscriptions' list (default: config.yaml)
    --output: Optional file to write YAML-formatted resource_sub_types list
"""

import sys
import argparse
import logging
import os
import yaml
from typing import Set
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
        sys.exit(1)

def load_config(config_path: str) -> dict:
    """Load config.yaml"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f) or {}


def render_resource_sub_types_yaml(resource_types: Set[str]) -> str:
    """Render resource sub-types as YAML list for direct paste into config.yaml."""
    lines = ["resource_sub_types:"]
    for rtype in sorted(resource_types):
        lines.append(f"  - {rtype}")
    return "\n".join(lines) + "\n"


def get_deployed_resource_types(subscriptions: list) -> Set[str]:
    """
    Query Azure Resource Graph to get all unique resource types/sub-types
    in the subscriptions.
    Returns: Set of resource types (normalized to lowercase)
    """
    try:
        credential = DefaultAzureCredential()
        ensure_azure_authentication(credential)
        client = ResourceGraphClient(credential)
        resource_types = set()
        for sub_id in subscriptions:
            query = QueryRequest(
                subscriptions=[sub_id],
                query="resources | distinct type"
            )
            result = client.resources(query)
            for row in result.data:
                resource_type = None
                if isinstance(row, dict) and 'type' in row:
                    resource_type = row['type']
                elif hasattr(row, 'type'):
                    resource_type = row.type

                if isinstance(resource_type, str) and resource_type:
                    normalized_type = resource_type.lower().strip()
                    if normalized_type:
                        resource_types.add(normalized_type)

        return resource_types
    except Exception as e:
        logger.error("Error querying Azure Resource Graph: %s", e)
        logger.error("Ensure you have Azure CLI authenticated: az login")
        sys.exit(1)

def main():
    log_level = os.getenv("GET_RESOURCES_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(levelname)s %(message)s"
    )
    if log_level != "DEBUG":
        logging.getLogger("azure").setLevel(logging.WARNING)
        logging.getLogger("azure.identity").setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(description="Extract deployed resource sub-types from Azure Resource Graph.")
    parser.add_argument('--config', type=str, default='config.yaml', help='Path to config YAML with subscriptions list')
    parser.add_argument('--output', type=str, help='Optional output file to write YAML-formatted resource sub-types')
    args = parser.parse_args()

    config = load_config(args.config)
    subscriptions = config.get("subscriptions", [])
    if not subscriptions:
        logger.error("No subscriptions configured in config.yaml")
        sys.exit(1)

    resource_types = get_deployed_resource_types(subscriptions)
    logger.info("Found %d unique deployed resource sub-types", len(resource_types))
    print(render_resource_sub_types_yaml(resource_types), end="")

    if args.output:
        with open(args.output, 'w') as f:
            f.write(render_resource_sub_types_yaml(resource_types))
        logger.info("YAML-formatted resource sub-types written to: %s", args.output)


if __name__ == "__main__":
    main()
