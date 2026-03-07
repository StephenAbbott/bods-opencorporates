"""Tests for person transformation (Officer → BODS Person Statement)."""

from bods_opencorporates.ingestion.models import OCAddress, OCOfficer
from bods_opencorporates.transform.persons import (
    build_person_addresses,
    build_person_names,
    transform_officer_person,
)


class TestBuildPersonNames:
    def test_full_name_with_parts(self, sample_officer_person):
        names = build_person_names(sample_officer_person)
        assert len(names) == 1
        assert names[0]["type"] == "legal"
        assert names[0]["fullName"] == "John Smith"
        assert names[0]["givenName"] == "John"
        assert names[0]["familyName"] == "Smith"

    def test_full_name_only(self):
        officer = OCOfficer(
            id="1", company_number="1", jurisdiction_code="gb",
            full_name="Jane Doe", position="Director",
        )
        names = build_person_names(officer)
        assert len(names) == 1
        assert names[0]["fullName"] == "Jane Doe"
        assert "givenName" not in names[0]
        assert "familyName" not in names[0]

    def test_empty_name(self):
        officer = OCOfficer(
            id="1", company_number="1", jurisdiction_code="gb",
            full_name="", position="Director",
        )
        names = build_person_names(officer)
        assert names == []


class TestBuildPersonAddresses:
    def test_with_address(self, sample_officer_person):
        addresses = build_person_addresses(sample_officer_person)
        assert len(addresses) == 1
        assert addresses[0]["type"] == "service"
        assert "456 High Street" in addresses[0]["address"]

    def test_country_of_residence_only(self):
        officer = OCOfficer(
            id="1", company_number="1", jurisdiction_code="gb",
            full_name="Test", position="Director",
            country_of_residence="France",
        )
        addresses = build_person_addresses(officer)
        assert len(addresses) == 1
        assert addresses[0]["type"] == "residence"
        assert addresses[0]["country"]["code"] == "FR"

    def test_no_address(self):
        officer = OCOfficer(
            id="1", company_number="1", jurisdiction_code="gb",
            full_name="Test", position="Director",
        )
        addresses = build_person_addresses(officer)
        assert addresses == []


class TestTransformOfficerPerson:
    def test_full_person(self, sample_officer_person, test_config):
        stmt = transform_officer_person(sample_officer_person, test_config)

        # Top-level
        assert stmt["recordType"] == "person"
        assert stmt["recordStatus"] == "new"
        assert "statementId" in stmt
        assert len(stmt["statementId"]) >= 32

        # Record details
        details = stmt["recordDetails"]
        assert details["isComponent"] is False
        assert details["personType"] == "knownPerson"

        # Names
        assert len(details["names"]) == 1
        assert details["names"][0]["fullName"] == "John Smith"
        assert details["names"][0]["givenName"] == "John"

        # Nationalities
        assert len(details["nationalities"]) == 1
        assert details["nationalities"][0]["code"] == "GB"

        # Birth date
        assert details["birthDate"] == "1975-03"

        # Addresses
        assert len(details["addresses"]) == 1

        # Publication details
        assert stmt["publicationDetails"]["bodsVersion"] == "0.4"

        # Source
        assert "officialRegister" in stmt["source"]["type"]

    def test_minimal_person(self, test_config):
        officer = OCOfficer(
            id="99",
            company_number="00001111",
            jurisdiction_code="fr",
            full_name="Pierre Dupont",
            position="Administrateur",
        )
        stmt = transform_officer_person(officer, test_config)

        assert stmt["recordType"] == "person"
        assert stmt["recordDetails"]["personType"] == "knownPerson"
        assert stmt["recordDetails"]["names"][0]["fullName"] == "Pierre Dupont"
        # No nationalities, birthDate, or addresses for minimal data

    def test_deterministic_ids(self, sample_officer_person, test_config):
        stmt1 = transform_officer_person(sample_officer_person, test_config)
        stmt2 = transform_officer_person(sample_officer_person, test_config)
        assert stmt1["statementId"] == stmt2["statementId"]
        assert stmt1["recordId"] == stmt2["recordId"]
