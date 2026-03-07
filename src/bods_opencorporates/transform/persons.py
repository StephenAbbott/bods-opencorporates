"""Transform OpenCorporates officer data into BODS person statements.

See BODS v0.4 person schema:
https://standard.openownership.org/en/0.4.0/standard/reference.html
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from bods_opencorporates.ingestion.models import OCOfficer
from bods_opencorporates.transform.identifiers import (
    company_record_id,
    generate_statement_id,
    officer_record_id,
)
from bods_opencorporates.utils.countries import resolve_country, resolve_nationalities
from bods_opencorporates.utils.dates import normalize_partial_date
from bods_opencorporates.utils.statements import (
    build_publication_details,
    build_source_officer,
    clean_statement,
)

if TYPE_CHECKING:
    from bods_opencorporates.config import PublisherConfig

logger = logging.getLogger(__name__)


def build_person_names(officer: OCOfficer) -> list[dict]:
    """Build BODS names array from an OpenCorporates officer.

    BODS requires at least a fullName. We also include givenName
    and familyName where available for structured name access.

    Returns:
        A list containing the legal name dict.
    """
    if not officer.full_name:
        return []

    name: dict = {
        "type": "legal",
        "fullName": officer.full_name.strip(),
    }

    if officer.first_name:
        name["givenName"] = officer.first_name.strip()
    if officer.last_name:
        name["familyName"] = officer.last_name.strip()

    return [name]


def build_person_addresses(officer: OCOfficer) -> list[dict]:
    """Build BODS addresses array from officer address and country of residence.

    Returns:
        A list of address dicts (may be empty).
    """
    addresses: list[dict] = []

    # Service address from officer's address field
    if officer.address:
        bods_address: dict = {"type": "service"}
        full = officer.address.full_address
        if full:
            bods_address["address"] = full
        if officer.address.postal_code:
            bods_address["postCode"] = officer.address.postal_code
        if officer.address.country:
            country = resolve_country(officer.address.country)
            if country:
                bods_address["country"] = country

        if bods_address.get("address") or bods_address.get("country"):
            addresses.append(bods_address)

    # Country of residence as a separate address if not already covered
    if officer.country_of_residence and not addresses:
        country = resolve_country(officer.country_of_residence)
        if country:
            addresses.append({
                "type": "residence",
                "country": country,
            })

    return addresses


def transform_officer_person(
    officer: OCOfficer,
    config: PublisherConfig,
) -> dict:
    """Transform an OpenCorporates officer into a BODS person statement.

    This should only be called for officers where officer_type is "Person"
    or None (natural persons). Corporate officers should use
    transform_corporate_officer_entity() instead.

    Args:
        officer: The OpenCorporates officer record.
        config: Publisher configuration for metadata fields.

    Returns:
        A complete BODS person statement dict.
    """
    rec_id = officer_record_id(
        officer.jurisdiction_code,
        officer.company_number,
        officer.id,
        person_uid=officer.person_uid,
    )
    company_rec_id = company_record_id(
        officer.jurisdiction_code, officer.company_number
    )

    statement = {
        "statementId": generate_statement_id(rec_id, config.publication_date, "new"),
        "declarationSubject": company_rec_id,
        "statementDate": config.publication_date,
        "recordId": rec_id,
        "recordType": "person",
        "recordStatus": "new",
        "recordDetails": {
            "isComponent": False,
            "personType": "knownPerson",
            "names": build_person_names(officer),
            "nationalities": resolve_nationalities(officer.nationality),
            "birthDate": normalize_partial_date(officer.partial_date_of_birth),
            "addresses": build_person_addresses(officer),
        },
        "publicationDetails": build_publication_details(config),
        "source": build_source_officer(
            officer.jurisdiction_code,
            officer.company_number,
            officer.id,
            config,
        ),
    }

    return clean_statement(statement)
