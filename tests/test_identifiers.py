"""Tests for identifier and statement ID generation."""

from bods_opencorporates.transform.identifiers import (
    build_entity_identifier,
    company_record_id,
    generate_statement_id,
    get_identifier_scheme,
    officer_record_id,
    relationship_record_id,
)


class TestCompanyRecordId:
    def test_basic(self):
        rid = company_record_id("gb", "00445790")
        assert rid == "openownership-register-gb-00445790"

    def test_subnational(self):
        rid = company_record_id("us_de", "12345")
        assert rid == "openownership-register-us_de-12345"

    def test_whitespace_stripped(self):
        rid = company_record_id("  gb  ", "  00445790  ")
        assert rid == "openownership-register-gb-00445790"


class TestOfficerRecordId:
    def test_without_person_uid(self):
        rid = officer_record_id("gb", "00445790", "12345")
        assert rid == "openownership-register-gb-00445790-officer-12345"

    def test_with_person_uid(self):
        rid = officer_record_id("gb", "00445790", "12345", person_uid="abc-123")
        assert rid == "openownership-register-person-abc-123"

    def test_person_uid_deduplicates(self):
        """Same person_uid should produce same record ID regardless of company."""
        rid1 = officer_record_id("gb", "00445790", "12345", person_uid="abc-123")
        rid2 = officer_record_id("gb", "99999999", "67890", person_uid="abc-123")
        assert rid1 == rid2


class TestRelationshipRecordId:
    def test_basic(self):
        rid = relationship_record_id("subject-id", "party-id")
        assert rid == "subject-id-rel-party-id"


class TestGenerateStatementId:
    def test_is_uuid_format(self):
        sid = generate_statement_id("record-1", "2024-01-15", "new")
        # UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        assert len(sid) == 36
        assert sid.count("-") == 4

    def test_deterministic(self):
        """Same inputs should produce the same statement ID."""
        sid1 = generate_statement_id("record-1", "2024-01-15", "new")
        sid2 = generate_statement_id("record-1", "2024-01-15", "new")
        assert sid1 == sid2

    def test_different_inputs(self):
        """Different inputs should produce different statement IDs."""
        sid1 = generate_statement_id("record-1", "2024-01-15", "new")
        sid2 = generate_statement_id("record-2", "2024-01-15", "new")
        sid3 = generate_statement_id("record-1", "2024-01-16", "new")
        sid4 = generate_statement_id("record-1", "2024-01-15", "updated")
        assert len({sid1, sid2, sid3, sid4}) == 4


class TestGetIdentifierScheme:
    def test_uk(self):
        scheme = get_identifier_scheme("gb")
        assert scheme == {"scheme": "GB-COH", "name": "Companies House"}

    def test_denmark(self):
        scheme = get_identifier_scheme("dk")
        assert scheme == {"scheme": "DK-CVR", "name": "Det Centrale Virksomhedsregister"}

    def test_us_delaware(self):
        scheme = get_identifier_scheme("us_de")
        assert scheme is not None
        assert "US" in scheme["scheme"]

    def test_unknown(self):
        scheme = get_identifier_scheme("xx")
        assert scheme is None

    def test_case_insensitive(self):
        scheme = get_identifier_scheme("GB")
        assert scheme == {"scheme": "GB-COH", "name": "Companies House"}


class TestBuildEntityIdentifier:
    def test_known_scheme(self):
        ident = build_entity_identifier("gb", "00445790")
        assert ident == {
            "id": "00445790",
            "scheme": "GB-COH",
            "schemeName": "Companies House",
        }

    def test_unknown_scheme(self):
        ident = build_entity_identifier("xx", "12345")
        assert ident == {"id": "12345"}
