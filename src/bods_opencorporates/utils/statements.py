"""Builders for BODS statement envelope fields.

These functions construct the shared metadata fields that appear on
every BODS statement: publicationDetails, source, and annotations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bods_opencorporates.config import PublisherConfig


def build_publication_details(config: PublisherConfig) -> dict:
    """Build the BODS publicationDetails object.

    This is required on every BODS statement and identifies who published
    the data, when, and under what license.
    """
    details: dict = {
        "publicationDate": config.publication_date,
        "bodsVersion": "0.4",
        "publisher": {
            "name": config.publisher_name,
        },
    }
    if config.publisher_uri:
        details["publisher"]["uri"] = config.publisher_uri
    if config.license_url:
        details["license"] = config.license_url
    return details


def build_source_company(
    jurisdiction_code: str,
    company_number: str,
    config: PublisherConfig,
) -> dict:
    """Build a BODS source object for an OpenCorporates company record.

    The source identifies where the data came from. OpenCorporates aggregates
    data from official company registers, so we mark the source type as
    'officialRegister' with OpenCorporates as the asserting party.
    """
    source: dict = {
        "type": ["officialRegister"],
        "description": "OpenCorporates - data sourced from official company registers",
        "url": f"https://opencorporates.com/companies/{jurisdiction_code}/{company_number}",
        "assertedBy": [
            {
                "name": "OpenCorporates",
                "uri": "https://opencorporates.com",
            }
        ],
    }
    if config.retrieved_at:
        source["retrievedAt"] = config.retrieved_at
    return source


def build_source_officer(
    jurisdiction_code: str,
    company_number: str,
    officer_id: str,
    config: PublisherConfig,
) -> dict:
    """Build a BODS source object for an OpenCorporates officer record."""
    source: dict = {
        "type": ["officialRegister"],
        "description": "OpenCorporates - officer data sourced from official company registers",
        "url": f"https://opencorporates.com/companies/{jurisdiction_code}/{company_number}/officers",
        "assertedBy": [
            {
                "name": "OpenCorporates",
                "uri": "https://opencorporates.com",
            }
        ],
    }
    if config.retrieved_at:
        source["retrievedAt"] = config.retrieved_at
    return source


def build_source_relationship(
    jurisdiction_code: str,
    company_number: str,
    config: PublisherConfig,
    description: str = "OpenCorporates - ownership data sourced from official company registers",
) -> dict:
    """Build a BODS source object for an OpenCorporates relationship record."""
    source: dict = {
        "type": ["officialRegister"],
        "description": description,
        "url": f"https://opencorporates.com/companies/{jurisdiction_code}/{company_number}",
        "assertedBy": [
            {
                "name": "OpenCorporates",
                "uri": "https://opencorporates.com",
            }
        ],
    }
    if config.retrieved_at:
        source["retrievedAt"] = config.retrieved_at
    return source


def clean_statement(statement: dict) -> dict:
    """Remove None values and empty collections from a BODS statement.

    BODS schema requires that optional fields are either present with valid
    values or absent entirely. This function strips out None values and
    empty lists/dicts to produce clean, valid BODS JSON.
    """
    cleaned = {}
    for key, value in statement.items():
        if value is None:
            continue
        if isinstance(value, dict):
            nested = clean_statement(value)
            if nested:  # Only include non-empty dicts
                cleaned[key] = nested
        elif isinstance(value, list):
            cleaned_list = []
            for item in value:
                if isinstance(item, dict):
                    nested_item = clean_statement(item)
                    if nested_item:
                        cleaned_list.append(nested_item)
                elif item is not None:
                    cleaned_list.append(item)
            if cleaned_list:  # Only include non-empty lists
                cleaned[key] = cleaned_list
        else:
            cleaned[key] = value
    return cleaned
