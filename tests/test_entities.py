"""Tests for entity transformation (Company → BODS Entity Statement)."""

from bods_opencorporates.ingestion.models import OCAddress, OCCompany
from bods_opencorporates.transform.entities import (
    build_entity_addresses,
    build_minimal_entity,
    map_entity_type,
    transform_company,
    transform_corporate_officer_entity,
)


class TestMapEntityType:
    def test_private_limited(self):
        result = map_entity_type("Private Limited Company")
        assert result == {"type": "registeredEntity"}

    def test_plc(self):
        result = map_entity_type("Public Limited Company")
        assert result == {"type": "registeredEntity"}

    def test_llc(self):
        result = map_entity_type("LLC")
        assert result == {"type": "registeredEntity"}

    def test_trust(self):
        result = map_entity_type("Unit Trust")
        assert result == {"type": "arrangement", "subtype": "trust"}

    def test_government(self):
        result = map_entity_type("Government Department")
        assert result == {"type": "stateBody", "subtype": "governmentDepartment"}

    def test_none(self):
        result = map_entity_type(None)
        assert result == {"type": "registeredEntity"}

    def test_unknown_type(self):
        result = map_entity_type("Some Unknown Type")
        assert result == {"type": "registeredEntity"}

    def test_case_insensitive(self):
        result = map_entity_type("PRIVATE LIMITED COMPANY")
        assert result["type"] == "registeredEntity"


class TestBuildEntityAddresses:
    def test_full_address(self, sample_address):
        addresses = build_entity_addresses(sample_address)
        assert len(addresses) == 1
        assert addresses[0]["type"] == "registered"
        assert "123 Main Street" in addresses[0]["address"]
        assert addresses[0]["postCode"] == "SW1A 1AA"
        assert addresses[0]["country"]["code"] == "GB"

    def test_no_address(self):
        assert build_entity_addresses(None) == []

    def test_country_only(self):
        address = OCAddress(country="Germany")
        addresses = build_entity_addresses(address)
        assert len(addresses) == 1
        assert addresses[0]["country"]["code"] == "DE"


class TestTransformCompany:
    def test_full_company(self, sample_company, test_config):
        stmt = transform_company(sample_company, test_config)

        # Top-level fields
        assert stmt["recordType"] == "entity"
        assert stmt["recordStatus"] == "new"
        assert stmt["statementDate"] == "2024-01-15"
        assert "statementId" in stmt
        assert len(stmt["statementId"]) >= 32

        # Record details
        details = stmt["recordDetails"]
        assert details["isComponent"] is False
        assert details["entityType"]["type"] == "registeredEntity"
        assert details["name"] == "TESCO PLC"
        assert details["foundingDate"] == "1947-11-21"
        assert "dissolutionDate" not in details  # None values stripped

        # Jurisdiction
        assert details["jurisdiction"]["code"] == "GB"

        # Identifiers
        assert len(details["identifiers"]) == 1
        assert details["identifiers"][0]["scheme"] == "GB-COH"
        assert details["identifiers"][0]["id"] == "00445790"

        # Addresses
        assert len(details["addresses"]) == 1
        assert details["addresses"][0]["type"] == "registered"

        # Publication details
        pub = stmt["publicationDetails"]
        assert pub["bodsVersion"] == "0.4"
        assert pub["publisher"]["name"] == "Test Publisher"

        # Source
        assert stmt["source"]["type"] == ["officialRegister"]
        assert "opencorporates.com" in stmt["source"]["url"]

        # Alternate names
        assert "TESCO STORES (HOLDINGS) LIMITED" in details["alternateNames"]

    def test_minimal_company(self, sample_company_minimal, test_config):
        stmt = transform_company(sample_company_minimal, test_config)

        assert stmt["recordType"] == "entity"
        assert stmt["recordDetails"]["name"] == "Test Danish Company ApS"
        assert stmt["recordDetails"]["jurisdiction"]["code"] == "DK"
        assert stmt["recordDetails"]["identifiers"][0]["scheme"] == "DK-CVR"

    def test_deterministic_ids(self, sample_company, test_config):
        """Same input should always produce the same statement ID."""
        stmt1 = transform_company(sample_company, test_config)
        stmt2 = transform_company(sample_company, test_config)
        assert stmt1["statementId"] == stmt2["statementId"]
        assert stmt1["recordId"] == stmt2["recordId"]


class TestBuildMinimalEntity:
    def test_minimal_entity(self, test_config):
        stmt = build_minimal_entity("gb", "99999999", "Minimal Co", test_config)

        assert stmt["recordType"] == "entity"
        assert stmt["recordDetails"]["name"] == "Minimal Co"
        assert stmt["recordDetails"]["entityType"]["type"] == "registeredEntity"
        assert stmt["recordDetails"]["identifiers"][0]["id"] == "99999999"
