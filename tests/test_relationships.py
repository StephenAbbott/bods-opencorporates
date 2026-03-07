"""Tests for relationship transformation."""

from bods_opencorporates.transform.identifiers import (
    company_record_id,
    officer_record_id,
)
from bods_opencorporates.transform.relationships import (
    transform_officer_relationship,
    transform_ownership_relationship,
)


class TestTransformOfficerRelationship:
    def test_director_relationship(self, sample_officer_person, test_config):
        comp_id = company_record_id("gb", "00445790")
        off_id = officer_record_id("gb", "00445790", "12345")

        stmt = transform_officer_relationship(
            sample_officer_person, comp_id, off_id, test_config
        )

        # Top-level
        assert stmt["recordType"] == "relationship"
        assert stmt["recordStatus"] == "new"
        assert stmt["declarationSubject"] == comp_id

        # Record details
        details = stmt["recordDetails"]
        assert details["isComponent"] is False
        assert details["subject"] == comp_id
        assert details["interestedParty"] == off_id

        # Interests
        assert len(details["interests"]) == 1
        assert details["interests"][0]["type"] == "appointmentOfBoard"
        assert details["interests"][0]["directOrIndirect"] == "direct"

    def test_deterministic_ids(self, sample_officer_person, test_config):
        comp_id = company_record_id("gb", "00445790")
        off_id = officer_record_id("gb", "00445790", "12345")

        stmt1 = transform_officer_relationship(
            sample_officer_person, comp_id, off_id, test_config
        )
        stmt2 = transform_officer_relationship(
            sample_officer_person, comp_id, off_id, test_config
        )
        assert stmt1["statementId"] == stmt2["statementId"]


class TestTransformOwnershipRelationship:
    def test_subsidiary(self, sample_relationship_subsidiary, test_config):
        stmt = transform_ownership_relationship(
            sample_relationship_subsidiary, test_config
        )

        assert stmt["recordType"] == "relationship"
        details = stmt["recordDetails"]
        assert details["isComponent"] is False

        # Subject and interested party should be record IDs
        assert "openownership-register-gb-00445790" in details["subject"]
        assert "openownership-register-gb-99887766" in details["interestedParty"]

        # Interests
        assert len(details["interests"]) == 1
        assert details["interests"][0]["type"] == "shareholding"
        assert details["interests"][0]["share"]["exact"] == 100.0
        assert details["interests"][0]["beneficialOwnershipOrControl"] is True

    def test_share_parcel_with_voting(self, sample_relationship_share, test_config):
        stmt = transform_ownership_relationship(
            sample_relationship_share, test_config
        )

        details = stmt["recordDetails"]
        assert len(details["interests"]) == 2

        types = {i["type"] for i in details["interests"]}
        assert "shareholding" in types
        assert "votingRights" in types

    def test_source_url(self, sample_relationship_subsidiary, test_config):
        stmt = transform_ownership_relationship(
            sample_relationship_subsidiary, test_config
        )
        assert "opencorporates.com" in stmt["source"]["url"]
