"""BODS statement and record ID generation, and identifier scheme mapping.

Record IDs are persistent identifiers for real-world entities, persons, and
relationships. They remain stable across updates.

Statement IDs are globally unique identifiers for each BODS statement.
They are generated deterministically from the record ID + statement date +
record status so that identical input data always produces identical output.

Identifier schemes map OpenCorporates jurisdiction codes to org-id.guide
scheme codes used in BODS entity identifiers.
"""

from __future__ import annotations

import hashlib
import uuid

# UUID v5 namespace for BODS OpenCorporates pipeline
BODS_OC_NAMESPACE = uuid.UUID("a7f3d2b1-8c4e-4f5a-9d6b-2e1c3f4a5b6c")

# Mapping of OpenCorporates jurisdiction codes to org-id.guide identifier schemes.
# See: https://org-id.guide/
JURISDICTION_SCHEMES: dict[str, dict[str, str]] = {
    # Europe
    "gb": {"scheme": "GB-COH", "name": "Companies House"},
    "ie": {"scheme": "IE-CRO", "name": "Companies Registration Office"},
    "fr": {"scheme": "FR-RCS", "name": "Registre du Commerce et des Societes"},
    "de": {"scheme": "DE-CR", "name": "Handelsregister"},
    "nl": {"scheme": "NL-KVK", "name": "Kamer van Koophandel"},
    "be": {"scheme": "BE-BCE_KBO", "name": "Banque-Carrefour des Entreprises"},
    "dk": {"scheme": "DK-CVR", "name": "Det Centrale Virksomhedsregister"},
    "se": {"scheme": "SE-BLV", "name": "Bolagsverket"},
    "no": {"scheme": "NO-BRC", "name": "Bronnoysundregistrene"},
    "fi": {"scheme": "FI-PRO", "name": "Patentti- ja Rekisterihallitus"},
    "it": {"scheme": "IT-RI", "name": "Registro Imprese"},
    "es": {"scheme": "ES-RMC", "name": "Registro Mercantil Central"},
    "pt": {"scheme": "PT-RCBE", "name": "Registo Central do Beneficiario Efetivo"},
    "at": {"scheme": "AT-FB", "name": "Firmenbuch"},
    "ch": {"scheme": "CH-FDJP", "name": "Eidgenossisches Amt fur das Handelsregister"},
    "pl": {"scheme": "PL-KRS", "name": "Krajowy Rejestr Sadowy"},
    "cz": {"scheme": "CZ-ICO", "name": "Registr ekonomickych subjektu"},
    "sk": {"scheme": "SK-ORSR", "name": "Obchodny Register Slovenskej Republiky"},
    "hu": {"scheme": "HU-AFA", "name": "Cegjegyzekszam"},
    "ro": {"scheme": "RO-CUI", "name": "Registrul Comertului"},
    "bg": {"scheme": "BG-EIK", "name": "BULSTAT Register"},
    "hr": {"scheme": "HR-MBS", "name": "Sudski Registar"},
    "si": {"scheme": "SI-PRS", "name": "Poslovni Register Slovenije"},
    "ee": {"scheme": "EE-RIK", "name": "Ariregistri Keskus"},
    "lv": {"scheme": "LV-RE", "name": "Latvijas Republikas Uznemumu Registrs"},
    "lt": {"scheme": "LT-RC", "name": "Registru Centras"},
    "cy": {"scheme": "CY-DRCOR", "name": "Department of Registrar of Companies"},
    "mt": {"scheme": "MT-MBR", "name": "Malta Business Registry"},
    "lu": {"scheme": "LU-RCS", "name": "Registre de Commerce et des Societes"},
    "li": {"scheme": "LI-OFCRR", "name": "Amt fur Justiz"},
    "is": {"scheme": "IS-RSK", "name": "Rikisskattstjori"},
    # Americas
    "us": {"scheme": "US-EIN", "name": "Employer Identification Number"},
    "us_de": {"scheme": "US-DOS", "name": "Delaware Division of Corporations"},
    "us_ca": {"scheme": "US-SOS-CA", "name": "California Secretary of State"},
    "us_ny": {"scheme": "US-DOS-NY", "name": "New York Division of Corporations"},
    "us_tx": {"scheme": "US-SOS-TX", "name": "Texas Secretary of State"},
    "us_fl": {"scheme": "US-DOS-FL", "name": "Florida Division of Corporations"},
    "us_wa": {"scheme": "US-SOS-WA", "name": "Washington Secretary of State"},
    "ca": {"scheme": "CA-CRA_ACR", "name": "Canada Revenue Agency"},
    "ca_bc": {"scheme": "CA-BC-REG", "name": "BC Registry Services"},
    "ca_on": {"scheme": "CA-ON-REG", "name": "Ontario Business Registry"},
    "ca_ab": {"scheme": "CA-AB-REG", "name": "Alberta Corporate Registry"},
    "br": {"scheme": "BR-CNPJ", "name": "Cadastro Nacional da Pessoa Juridica"},
    "mx": {"scheme": "MX-RFC", "name": "Registro Federal de Contribuyentes"},
    "co": {"scheme": "CO-RUE", "name": "Registro Unico Empresarial"},
    "ar": {"scheme": "AR-CUIT", "name": "Clave Unica de Identificacion Tributaria"},
    # Asia-Pacific
    "au": {"scheme": "AU-ABN", "name": "Australian Business Number"},
    "nz": {"scheme": "NZ-NZBN", "name": "New Zealand Business Number"},
    "jp": {"scheme": "JP-JCN", "name": "Corporate Number"},
    "kr": {"scheme": "KR-CRN", "name": "Corporate Registration Number"},
    "hk": {"scheme": "HK-CR", "name": "Companies Registry"},
    "sg": {"scheme": "SG-ACRA", "name": "Accounting and Corporate Regulatory Authority"},
    "in": {"scheme": "IN-MCA", "name": "Ministry of Corporate Affairs"},
    "cn": {"scheme": "CN-SAIC", "name": "State Administration for Industry and Commerce"},
    "my": {"scheme": "MY-SSM", "name": "Suruhanjaya Syarikat Malaysia"},
    "id": {"scheme": "ID-AHU", "name": "Administrasi Hukum Umum"},
    "ph": {"scheme": "PH-SEC", "name": "Securities and Exchange Commission"},
    "th": {"scheme": "TH-DBD", "name": "Department of Business Development"},
    # Africa
    "za": {"scheme": "ZA-CIPC", "name": "Companies and Intellectual Property Commission"},
    "ng": {"scheme": "NG-CAC", "name": "Corporate Affairs Commission"},
    "ke": {"scheme": "KE-RCO", "name": "Registrar of Companies"},
    "gh": {"scheme": "GH-RGD", "name": "Registrar Generals Department"},
    # Middle East
    "ae": {"scheme": "AE-ADELS", "name": "Abu Dhabi Department of Economic Development"},
    "sa": {"scheme": "SA-MCI", "name": "Ministry of Commerce and Investment"},
    "il": {"scheme": "IL-ROC", "name": "Registrar of Companies"},
    # Caribbean / Offshore
    "ky": {"scheme": "KY-CR", "name": "Cayman Islands General Registry"},
    "bm": {"scheme": "BM-ROC", "name": "Bermuda Registrar of Companies"},
    "vg": {"scheme": "VG-FSC", "name": "BVI Financial Services Commission"},
    "gg": {"scheme": "GG-GREG", "name": "Guernsey Registry"},
    "je": {"scheme": "JE-FSC", "name": "Jersey Financial Services Commission"},
    "im": {"scheme": "IM-CR", "name": "Isle of Man Companies Registry"},
    "gi": {"scheme": "GI-CR", "name": "Gibraltar Companies House"},
}


def company_record_id(jurisdiction_code: str, company_number: str) -> str:
    """Generate a deterministic BODS record ID for a company.

    Format: openownership-register-{jurisdiction}-{number}
    """
    jur = jurisdiction_code.strip().lower()
    num = company_number.strip()
    return f"openownership-register-{jur}-{num}"


def officer_record_id(
    jurisdiction_code: str,
    company_number: str,
    officer_id: str,
    person_uid: str | None = None,
) -> str:
    """Generate a deterministic BODS record ID for an officer.

    If a person_uid is available (OpenCorporates person deduplication ID),
    use it for cross-company deduplication. Otherwise, scope to the company.
    """
    if person_uid:
        return f"openownership-register-person-{person_uid}"

    jur = jurisdiction_code.strip().lower()
    num = company_number.strip()
    oid = str(officer_id).strip()
    return f"openownership-register-{jur}-{num}-officer-{oid}"


def relationship_record_id(subject_record_id: str, interested_party_record_id: str) -> str:
    """Generate a deterministic BODS record ID for a relationship.

    Combines the subject and interested party record IDs to create
    a unique relationship identifier.
    """
    return f"{subject_record_id}-rel-{interested_party_record_id}"


def generate_statement_id(
    record_id: str,
    statement_date: str,
    record_status: str = "new",
) -> str:
    """Generate a deterministic globally-unique BODS statement ID.

    Uses UUID v5 (SHA-1 based, deterministic) so the same inputs
    always produce the same statement ID.

    Returns:
        A UUID string (36 chars including hyphens, within 32-64 char requirement).
    """
    name = f"{record_id}:{statement_date}:{record_status}"
    return str(uuid.uuid5(BODS_OC_NAMESPACE, name))


def get_identifier_scheme(jurisdiction_code: str) -> dict | None:
    """Look up the org-id.guide identifier scheme for a jurisdiction.

    Args:
        jurisdiction_code: OpenCorporates jurisdiction code (e.g., 'gb', 'us_de').

    Returns:
        {"scheme": "GB-COH", "name": "Companies House"} or None.
    """
    code = jurisdiction_code.strip().lower()
    return JURISDICTION_SCHEMES.get(code)


def build_entity_identifier(
    jurisdiction_code: str,
    company_number: str,
) -> dict | None:
    """Build a BODS identifier object for a company entity.

    Returns:
        {"scheme": "GB-COH", "id": "12345678", "schemeName": "Companies House"}
        or a minimal {"id": "12345678"} if no scheme is known.
    """
    scheme_info = get_identifier_scheme(jurisdiction_code)
    identifier: dict = {"id": company_number.strip()}

    if scheme_info:
        identifier["scheme"] = scheme_info["scheme"]
        identifier["schemeName"] = scheme_info["name"]

    return identifier
