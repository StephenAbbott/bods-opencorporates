"""Transform OpenCorporates company data into BODS entity statements.

See BODS v0.4 entity schema:
https://standard.openownership.org/en/0.4.0/standard/reference.html
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from bods_opencorporates.ingestion.models import OCAddress, OCCompany, OCOfficer
from bods_opencorporates.transform.identifiers import (
    build_entity_identifier,
    company_record_id,
    generate_statement_id,
)
from bods_opencorporates.utils.countries import resolve_country, resolve_jurisdiction
from bods_opencorporates.utils.dates import normalize_date
from bods_opencorporates.utils.statements import (
    build_publication_details,
    build_source_company,
    clean_statement,
)

if TYPE_CHECKING:
    from bods_opencorporates.config import PublisherConfig

logger = logging.getLogger(__name__)


# Mapping of common OpenCorporates company_type values to BODS entityType
COMPANY_TYPE_MAP: dict[str, dict] = {
    # Standard registered entities
    "private limited company": {"type": "registeredEntity"},
    "public limited company": {"type": "registeredEntity"},
    "limited liability company": {"type": "registeredEntity"},
    "llc": {"type": "registeredEntity"},
    "ltd": {"type": "registeredEntity"},
    "plc": {"type": "registeredEntity"},
    "corporation": {"type": "registeredEntity"},
    "inc": {"type": "registeredEntity"},
    "incorporated": {"type": "registeredEntity"},
    "limited partnership": {"type": "registeredEntity"},
    "general partnership": {"type": "registeredEntity"},
    "limited liability partnership": {"type": "registeredEntity"},
    "llp": {"type": "registeredEntity"},
    "sole proprietorship": {"type": "registeredEntity"},
    "cooperative": {"type": "registeredEntity"},
    "societe anonyme": {"type": "registeredEntity"},
    "sa": {"type": "registeredEntity"},
    "sarl": {"type": "registeredEntity"},
    "sas": {"type": "registeredEntity"},
    "gmbh": {"type": "registeredEntity"},
    "ag": {"type": "registeredEntity"},
    "bv": {"type": "registeredEntity"},
    "nv": {"type": "registeredEntity"},
    "ab": {"type": "registeredEntity"},
    "aps": {"type": "registeredEntity"},
    "as": {"type": "registeredEntity"},
    "oy": {"type": "registeredEntity"},
    "srl": {"type": "registeredEntity"},
    "spa": {"type": "registeredEntity"},
    "sl": {"type": "registeredEntity"},
    # State/government
    "government": {"type": "stateBody", "subtype": "governmentDepartment"},
    "government department": {"type": "stateBody", "subtype": "governmentDepartment"},
    "state agency": {"type": "stateBody", "subtype": "stateAgency"},
    "public body": {"type": "stateBody"},
    "statutory body": {"type": "stateBody"},
    "crown entity": {"type": "stateBody"},
    # Non-profit / charity
    "charity": {"type": "registeredEntity"},
    "not-for-profit": {"type": "registeredEntity"},
    "nonprofit": {"type": "registeredEntity"},
    "community interest company": {"type": "registeredEntity"},
    "cic": {"type": "registeredEntity"},
    "foundation": {"type": "registeredEntity"},
    "association": {"type": "registeredEntity"},
    # Trust / arrangement
    "trust": {"type": "arrangement", "subtype": "trust"},
    "unit trust": {"type": "arrangement", "subtype": "trust"},
    # Foreign / branch
    "foreign company": {"type": "registeredEntity"},
    "branch": {"type": "registeredEntity"},
    "overseas company": {"type": "registeredEntity"},
}


def map_entity_type(company_type: str | None) -> dict:
    """Map an OpenCorporates company_type to a BODS entityType object.

    Args:
        company_type: The company_type string from OpenCorporates.

    Returns:
        A BODS entityType dict, defaulting to registeredEntity.
    """
    if not company_type:
        return {"type": "registeredEntity"}

    normalized = company_type.strip().lower()

    # Exact match
    if normalized in COMPANY_TYPE_MAP:
        return dict(COMPANY_TYPE_MAP[normalized])

    # Substring match for common patterns
    if "trust" in normalized:
        return {"type": "arrangement", "subtype": "trust"}
    if "government" in normalized or "state" in normalized or "crown" in normalized:
        return {"type": "stateBody"}
    if any(kw in normalized for kw in ("ltd", "limited", "inc", "corp", "company")):
        return {"type": "registeredEntity"}

    return {"type": "registeredEntity"}


def build_entity_addresses(address: OCAddress | None) -> list[dict]:
    """Build BODS addresses array from an OpenCorporates address.

    Returns:
        A list containing one address dict, or empty list if no address.
    """
    if not address:
        return []

    bods_address: dict = {"type": "registered"}

    # Build the address string from components
    full = address.full_address
    if full:
        bods_address["address"] = full

    if address.postal_code:
        bods_address["postCode"] = address.postal_code

    if address.country:
        country = resolve_country(address.country)
        if country:
            bods_address["country"] = country

    if not bods_address.get("address") and not bods_address.get("country"):
        return []

    return [bods_address]


def transform_company(company: OCCompany, config: PublisherConfig) -> dict:
    """Transform an OpenCorporates company into a BODS entity statement.

    Args:
        company: The OpenCorporates company record.
        config: Publisher configuration for metadata fields.

    Returns:
        A complete BODS entity statement dict.
    """
    record_id = company_record_id(company.jurisdiction_code, company.company_number)

    statement = {
        "statementId": generate_statement_id(record_id, config.publication_date, "new"),
        "declarationSubject": record_id,
        "statementDate": config.publication_date,
        "recordId": record_id,
        "recordType": "entity",
        "recordStatus": "new",
        "recordDetails": {
            "isComponent": False,
            "entityType": map_entity_type(company.company_type),
            "name": company.name,
            "jurisdiction": resolve_jurisdiction(company.jurisdiction_code),
            "identifiers": [
                build_entity_identifier(
                    company.jurisdiction_code, company.company_number
                ),
            ],
            "foundingDate": normalize_date(company.incorporation_date),
            "dissolutionDate": normalize_date(company.dissolution_date),
            "addresses": build_entity_addresses(company.registered_address),
        },
        "publicationDetails": build_publication_details(config),
        "source": build_source_company(
            company.jurisdiction_code,
            company.company_number,
            config,
        ),
    }

    # Add alternate names if there are previous names
    if company.previous_names:
        statement["recordDetails"]["alternateNames"] = company.previous_names

    return clean_statement(statement)


def transform_corporate_officer_entity(
    officer: OCOfficer,
    config: PublisherConfig,
) -> dict:
    """Transform a corporate officer (type=Company) into a BODS entity statement.

    When an officer is itself a company (corporate director), we create
    an entity statement for it rather than a person statement.

    Args:
        officer: The OpenCorporates officer record with officer_type="Company".
        config: Publisher configuration.

    Returns:
        A BODS entity statement dict for the corporate officer.
    """
    # Try to use the officer's own company details if available
    record_id = company_record_id(
        officer.jurisdiction_code, officer.id
    )

    statement = {
        "statementId": generate_statement_id(record_id, config.publication_date, "new"),
        "declarationSubject": company_record_id(
            officer.jurisdiction_code, officer.company_number
        ),
        "statementDate": config.publication_date,
        "recordId": record_id,
        "recordType": "entity",
        "recordStatus": "new",
        "recordDetails": {
            "isComponent": False,
            "entityType": {"type": "registeredEntity"},
            "name": officer.full_name,
            "addresses": build_entity_addresses(officer.address),
        },
        "publicationDetails": build_publication_details(config),
        "source": build_source_company(
            officer.jurisdiction_code,
            officer.company_number,
            config,
        ),
    }

    return clean_statement(statement)


def build_minimal_entity(
    jurisdiction_code: str,
    company_number: str,
    name: str,
    config: PublisherConfig,
) -> dict:
    """Build a minimal BODS entity statement for a referenced company.

    Used when processing relationships where we need to reference an
    entity that hasn't been fully ingested (e.g., from the relationships
    CSV where we only have the company number and name).
    """
    record_id = company_record_id(jurisdiction_code, company_number)

    statement = {
        "statementId": generate_statement_id(record_id, config.publication_date, "new"),
        "declarationSubject": record_id,
        "statementDate": config.publication_date,
        "recordId": record_id,
        "recordType": "entity",
        "recordStatus": "new",
        "recordDetails": {
            "isComponent": False,
            "entityType": {"type": "registeredEntity"},
            "name": name,
            "jurisdiction": resolve_jurisdiction(jurisdiction_code),
            "identifiers": [
                build_entity_identifier(jurisdiction_code, company_number),
            ],
        },
        "publicationDetails": build_publication_details(config),
        "source": build_source_company(jurisdiction_code, company_number, config),
    }

    return clean_statement(statement)
