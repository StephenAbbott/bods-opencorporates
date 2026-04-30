"""Interest type mapping for BODS relationship statements.

Maps OpenCorporates officer positions and ownership data to BODS
interest types from the interestType codelist.

See: https://standard.openownership.org/en/0.4.0/standard/reference.html
"""

from __future__ import annotations

import logging
import re

from bods_opencorporates.ingestion.models import OCOfficer, OCRelationship
from bods_opencorporates.utils.dates import normalize_date

logger = logging.getLogger(__name__)


# Mapping of officer position strings (lowercased) to BODS interest types.
# Positions are matched case-insensitively with substring matching as fallback.
POSITION_TO_INTEREST_TYPE: dict[str, str] = {
    # Board-level appointments
    "director": "appointmentOfBoard",
    "managing director": "appointmentOfBoard",
    "executive director": "appointmentOfBoard",
    "non-executive director": "appointmentOfBoard",
    "alternate director": "appointmentOfBoard",
    "shadow director": "appointmentOfBoard",
    "de facto director": "appointmentOfBoard",
    "deputy director": "appointmentOfBoard",
    "associate director": "appointmentOfBoard",
    "joint director": "appointmentOfBoard",
    "directeur": "appointmentOfBoard",  # French
    "directeur general": "appointmentOfBoard",
    "geschaeftsfuehrer": "appointmentOfBoard",  # German
    "direktor": "appointmentOfBoard",  # German/Nordic
    "bestuurder": "appointmentOfBoard",  # Dutch
    "amministratore": "appointmentOfBoard",  # Italian
    "administrador": "appointmentOfBoard",  # Spanish/Portuguese
    # Board membership
    "board member": "boardMember",
    "member of the board": "boardMember",
    "supervisory board member": "boardMember",
    "aufsichtsratsmitglied": "boardMember",  # German
    "bestuurslid": "boardMember",  # Dutch
    # Board chair
    "chairman": "boardChair",      # was "boardMember"
    "chairwoman": "boardChair",    # was "boardMember"
    "chairperson": "boardChair",   # was "boardMember"
    "chair": "boardChair",         # was "boardMember"
    "president": "boardChair",     # was "boardMember"
    "vice president": "boardMember",
    "vice chairman": "boardChair", # was "boardMember"
    "deputy chairman": "boardChair", # was "boardMember"
    "vorsitzender": "boardMember",  # German
    "voorzitter": "boardMember",  # Dutch
    "presidente": "boardMember",  # Italian/Spanish/Portuguese
    # Senior management
    "secretary": "seniorManagingOfficial",
    "company secretary": "seniorManagingOfficial",
    "corporate secretary": "seniorManagingOfficial",
    "assistant secretary": "seniorManagingOfficial",
    "joint secretary": "seniorManagingOfficial",
    "chief executive": "seniorManagingOfficial",
    "chief executive officer": "seniorManagingOfficial",
    "ceo": "seniorManagingOfficial",
    "chief financial officer": "seniorManagingOfficial",
    "cfo": "seniorManagingOfficial",
    "chief operating officer": "seniorManagingOfficial",
    "coo": "seniorManagingOfficial",
    "chief technology officer": "seniorManagingOfficial",
    "cto": "seniorManagingOfficial",
    "treasurer": "seniorManagingOfficial",
    "manager": "seniorManagingOfficial",
    "general manager": "seniorManagingOfficial",
    "partner": "seniorManagingOfficial",
    "general partner": "seniorManagingOfficial",
    "limited partner": "seniorManagingOfficial",
    "managing partner": "seniorManagingOfficial",
    "member": "seniorManagingOfficial",
    "managing member": "seniorManagingOfficial",
    "liquidator": "seniorManagingOfficial",
    "receiver": "seniorManagingOfficial",
    "administrator": "seniorManagingOfficial",
    "gerant": "seniorManagingOfficial",  # French
    # Nominee/agent
    "nominee": "nominee",
    "nominee director": "nominee",
    "nominee shareholder": "nominee",
    "nominee secretary": "nominee",
    "agent": "otherInfluenceOrControl",
    "authorized representative": "otherInfluenceOrControl",
    "authorised representative": "otherInfluenceOrControl",
    "representative": "otherInfluenceOrControl",
    "legal representative": "otherInfluenceOrControl",
    "proxy": "otherInfluenceOrControl",
    "power of attorney": "otherInfluenceOrControl",
    # Trust-related
    "trustee": "trustee",
    "co-trustee": "trustee",
    "settlor": "settlor",
    "protector": "protector",
    "beneficiary": "beneficiaryOfLegalArrangement",
    "guardian": "otherInfluenceOrControl",
    # Ownership
    "shareholder": "shareholding",
    "owner": "shareholding",
    "subscriber": "shareholding",
    "incorporator": "otherInfluenceOrControl",
    "founder": "otherInfluenceOrControl",
}

def match_position(position: str) -> str:
    """Match an officer position string to a BODS interest type.

    Uses exact match first, then substring matching, then defaults to
    otherInfluenceOrControl.

    Args:
        position: Officer position string from OpenCorporates.

    Returns:
        A valid BODS interestType codelist value.
    """
    if not position:
        return "unknownInterest"

    normalized = position.strip().lower()

    # Exact match
    if normalized in POSITION_TO_INTEREST_TYPE:
        return POSITION_TO_INTEREST_TYPE[normalized]

    # Substring match: check if any known position is contained in the input
    for known_position, interest_type in POSITION_TO_INTEREST_TYPE.items():
        if known_position in normalized:
            return interest_type

    # Check for common patterns
    if re.search(r"\bdirect(or|eur|ör)\b", normalized, re.IGNORECASE):
        return "appointmentOfBoard"
    if re.search(r"\bsecretar", normalized, re.IGNORECASE):
        return "seniorManagingOfficial"
    if re.search(r"\bmanag", normalized, re.IGNORECASE):
        return "seniorManagingOfficial"
    if re.search(r"\bchair", normalized, re.IGNORECASE):
        return "boardChair"  # was "boardMember"
    if re.search(r"\btrustee", normalized, re.IGNORECASE):
        return "trustee"
    if re.search(r"\bnominee", normalized, re.IGNORECASE):
        return "nominee"

    logger.warning("Unknown officer position '%s', defaulting to otherInfluenceOrControl", position)
    return "otherInfluenceOrControl"


def is_beneficial_position(interest_type: str) -> bool:
    """Determine if an interest type constitutes beneficial ownership.

    Note: Officer positions like board appointments and senior management
    represent control but are not automatically beneficial ownership.
    In OpenCorporates data, officer appointments should not be marked
    as beneficialOwnershipOrControl=True because they represent governance
    roles, not ownership claims. Only shareholding/voting rights from
    the relationships data should be marked as beneficial ownership.
    """
    return interest_type in {"shareholding", "votingRights"}


def map_officer_interest(officer: OCOfficer) -> dict:
    """Map an officer appointment to a BODS interest object.

    Args:
        officer: The OpenCorporates officer record.

    Returns:
        A BODS interest dict with type, directOrIndirect, dates, etc.
    """
    interest_type = match_position(officer.position)

    interest: dict = {
        "type": interest_type,
        "directOrIndirect": "direct",
        "beneficialOwnershipOrControl": is_beneficial_position(interest_type),
    }

    start = normalize_date(officer.start_date)
    if start:
        interest["startDate"] = start

    end = normalize_date(officer.end_date)
    if end:
        interest["endDate"] = end

    # Add position details if we couldn't cleanly map the type
    if interest_type == "otherInfluenceOrControl" and officer.position:
        interest["details"] = f"Officer position: {officer.position}"

    return interest


def build_share(
    minimum: float | None,
    maximum: float | None,
) -> dict:
    """Build a BODS share object from min/max percentage values.

    Returns:
        {"exact": 50} if min == max, or {"minimum": 25, "maximum": 50}.
    """
    share: dict = {}

    if minimum is not None and maximum is not None:
        if minimum == maximum:
            share["exact"] = minimum
        else:
            share["minimum"] = minimum
            share["maximum"] = maximum
    elif minimum is not None:
        share["minimum"] = minimum
    elif maximum is not None:
        share["maximum"] = maximum

    return share


def map_ownership_interests(rel: OCRelationship) -> list[dict]:
    """Map an ownership/control relationship to BODS interest objects.

    A single OC relationship may produce multiple BODS interests
    (e.g., both shareholding and voting rights).

    Args:
        rel: The OpenCorporates relationship record.

    Returns:
        A list of BODS interest dicts.
    """
    interests: list[dict] = []

    # Shareholding interest
    if rel.percentage_min_share_ownership is not None or rel.percentage_max_share_ownership is not None:
        interest: dict = {
            "type": "shareholding",
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": True,
            "share": build_share(
                rel.percentage_min_share_ownership,
                rel.percentage_max_share_ownership,
            ),
        }
        start = normalize_date(rel.start_date)
        if start:
            interest["startDate"] = start
        end = normalize_date(rel.end_date)
        if end:
            interest["endDate"] = end
        interests.append(interest)

    # Voting rights interest
    if rel.percentage_min_voting_rights is not None or rel.percentage_max_voting_rights is not None:
        interest = {
            "type": "votingRights",
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": True,
            "share": build_share(
                rel.percentage_min_voting_rights,
                rel.percentage_max_voting_rights,
            ),
        }
        start = normalize_date(rel.start_date)
        if start:
            interest["startDate"] = start
        end = normalize_date(rel.end_date)
        if end:
            interest["endDate"] = end
        interests.append(interest)

    # If it's a subsidiary/control relationship with no percentage data
    if not interests:
        interest_type = _map_relationship_type(rel.relationship_type)
        interest = {
            "type": interest_type,
            "directOrIndirect": "direct",
            "beneficialOwnershipOrControl": interest_type in {"shareholding", "votingRights"},
        }
        start = normalize_date(rel.start_date)
        if start:
            interest["startDate"] = start
        end = normalize_date(rel.end_date)
        if end:
            interest["endDate"] = end
        interests.append(interest)

    return interests


def _map_relationship_type(relationship_type: str) -> str:
    """Map an OpenCorporates relationship type to a BODS interest type."""
    type_lower = (relationship_type or "").strip().lower()

    if type_lower in ("control_statement", "control"):
        return "otherInfluenceOrControl"
    if type_lower in ("subsidiary", "parent"):
        return "shareholding"
    if type_lower in ("branch",):
        return "otherInfluenceOrControl"
    if type_lower in ("share_parcel", "share"):
        return "shareholding"

    return "otherInfluenceOrControl"
