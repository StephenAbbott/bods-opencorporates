"""Transform OpenCorporates officer and ownership data into BODS relationship statements.

See BODS v0.4 relationship schema:
https://standard.openownership.org/en/0.4.0/standard/reference.html
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from bods_opencorporates.ingestion.models import OCOfficer, OCRelationship
from bods_opencorporates.transform.identifiers import (
    company_record_id,
    generate_statement_id,
    officer_record_id,
    relationship_record_id,
)
from bods_opencorporates.transform.interests import (
    map_officer_interest,
    map_ownership_interests,
)
from bods_opencorporates.utils.statements import (
    build_publication_details,
    build_source_officer,
    build_source_relationship,
    clean_statement,
)

if TYPE_CHECKING:
    from bods_opencorporates.config import PublisherConfig

logger = logging.getLogger(__name__)


def transform_officer_relationship(
    officer: OCOfficer,
    company_rec_id: str,
    officer_rec_id: str,
    config: PublisherConfig,
) -> dict:
    """Transform an officer appointment into a BODS relationship statement.

    The relationship links the company (subject) to the officer
    (interestedParty) with an interest derived from their position.

    Args:
        officer: The OpenCorporates officer record.
        company_rec_id: BODS record ID of the company entity.
        officer_rec_id: BODS record ID of the officer (person or entity).
        config: Publisher configuration for metadata fields.

    Returns:
        A complete BODS relationship statement dict.
    """
    rel_rec_id = relationship_record_id(company_rec_id, officer_rec_id)

    statement = {
        "statementId": generate_statement_id(rel_rec_id, config.publication_date, "new"),
        "declarationSubject": company_rec_id,
        "statementDate": config.publication_date,
        "recordId": rel_rec_id,
        "recordType": "relationship",
        "recordStatus": "new",
        "recordDetails": {
            "isComponent": False,
            "subject": company_rec_id,
            "interestedParty": officer_rec_id,
            "interests": [map_officer_interest(officer)],
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


def transform_ownership_relationship(
    rel: OCRelationship,
    config: PublisherConfig,
) -> dict:
    """Transform a corporate ownership relationship into a BODS relationship statement.

    Maps OpenCorporates relationship types (control_statement, subsidiary,
    share_parcel, branch) to BODS relationship statements with appropriate
    interest types and share percentages.

    Args:
        rel: The OpenCorporates relationship record.
        config: Publisher configuration for metadata fields.

    Returns:
        A complete BODS relationship statement dict.
    """
    subject_id = company_record_id(
        rel.subject_jurisdiction_code, rel.subject_company_number
    )
    object_id = company_record_id(
        rel.object_jurisdiction_code, rel.object_company_number
    )
    rel_rec_id = relationship_record_id(subject_id, object_id)

    statement = {
        "statementId": generate_statement_id(rel_rec_id, config.publication_date, "new"),
        "declarationSubject": subject_id,
        "statementDate": config.publication_date,
        "recordId": rel_rec_id,
        "recordType": "relationship",
        "recordStatus": "new",
        "recordDetails": {
            "isComponent": False,
            "subject": subject_id,
            "interestedParty": object_id,
            "interests": map_ownership_interests(rel),
        },
        "publicationDetails": build_publication_details(config),
        "source": build_source_relationship(
            rel.subject_jurisdiction_code,
            rel.subject_company_number,
            config,
        ),
    }

    return clean_statement(statement)
