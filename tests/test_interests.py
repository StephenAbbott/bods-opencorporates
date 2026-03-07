"""Tests for interest type mapping."""

from bods_opencorporates.ingestion.models import OCOfficer, OCRelationship
from bods_opencorporates.transform.interests import (
    build_share,
    is_beneficial_position,
    map_officer_interest,
    map_ownership_interests,
    match_position,
)


class TestMatchPosition:
    def test_director(self):
        assert match_position("Director") == "appointmentOfBoard"

    def test_managing_director(self):
        assert match_position("Managing Director") == "appointmentOfBoard"

    def test_secretary(self):
        assert match_position("Company Secretary") == "seniorManagingOfficial"

    def test_ceo(self):
        assert match_position("CEO") == "seniorManagingOfficial"

    def test_chairman(self):
        assert match_position("Chairman") == "boardMember"

    def test_trustee(self):
        assert match_position("Trustee") == "trustee"

    def test_nominee(self):
        assert match_position("Nominee Director") == "nominee"

    def test_shareholder(self):
        assert match_position("Shareholder") == "shareholding"

    def test_unknown_position(self):
        assert match_position("Janitor") == "otherInfluenceOrControl"

    def test_empty_position(self):
        assert match_position("") == "unknownInterest"

    def test_case_insensitive(self):
        assert match_position("DIRECTOR") == "appointmentOfBoard"
        assert match_position("company secretary") == "seniorManagingOfficial"

    def test_substring_match(self):
        """Should match known positions as substrings."""
        assert match_position("Executive Director of Operations") == "appointmentOfBoard"
        assert match_position("Assistant Company Secretary") == "seniorManagingOfficial"

    def test_german_position(self):
        assert match_position("Geschaeftsfuehrer") == "appointmentOfBoard"

    def test_french_position(self):
        assert match_position("Directeur General") == "appointmentOfBoard"


class TestIsBeneficialPosition:
    def test_shareholding_is_beneficial(self):
        assert is_beneficial_position("shareholding") is True

    def test_voting_rights_is_beneficial(self):
        assert is_beneficial_position("votingRights") is True

    def test_board_appointment_not_beneficial(self):
        assert is_beneficial_position("appointmentOfBoard") is False

    def test_senior_management_not_beneficial(self):
        assert is_beneficial_position("seniorManagingOfficial") is False


class TestBuildShare:
    def test_exact_share(self):
        share = build_share(50.0, 50.0)
        assert share == {"exact": 50.0}

    def test_range_share(self):
        share = build_share(25.0, 50.0)
        assert share == {"minimum": 25.0, "maximum": 50.0}

    def test_minimum_only(self):
        share = build_share(25.0, None)
        assert share == {"minimum": 25.0}

    def test_maximum_only(self):
        share = build_share(None, 75.0)
        assert share == {"maximum": 75.0}

    def test_no_values(self):
        share = build_share(None, None)
        assert share == {}


class TestMapOfficerInterest:
    def test_director_interest(self, sample_officer_person):
        interest = map_officer_interest(sample_officer_person)
        assert interest["type"] == "appointmentOfBoard"
        assert interest["directOrIndirect"] == "direct"
        assert interest["beneficialOwnershipOrControl"] is False
        assert interest["startDate"] == "2020-01-15"
        assert "endDate" not in interest

    def test_secretary_interest(self, sample_officer_secretary):
        interest = map_officer_interest(sample_officer_secretary)
        assert interest["type"] == "seniorManagingOfficial"
        assert interest["startDate"] == "2019-06-01"


class TestMapOwnershipInterests:
    def test_subsidiary_with_share(self, sample_relationship_subsidiary):
        interests = map_ownership_interests(sample_relationship_subsidiary)
        assert len(interests) == 1
        assert interests[0]["type"] == "shareholding"
        assert interests[0]["share"]["exact"] == 100.0
        assert interests[0]["beneficialOwnershipOrControl"] is True

    def test_share_with_voting_rights(self, sample_relationship_share):
        interests = map_ownership_interests(sample_relationship_share)
        assert len(interests) == 2

        share_interest = interests[0]
        assert share_interest["type"] == "shareholding"
        assert share_interest["share"]["minimum"] == 25.0
        assert share_interest["share"]["maximum"] == 50.0

        voting_interest = interests[1]
        assert voting_interest["type"] == "votingRights"
        assert voting_interest["share"]["minimum"] == 25.0

    def test_relationship_without_percentages(self):
        rel = OCRelationship(
            relationship_type="subsidiary",
            subject_company_number="111",
            subject_jurisdiction_code="gb",
            subject_name="Parent",
            object_company_number="222",
            object_jurisdiction_code="gb",
            object_name="Child",
        )
        interests = map_ownership_interests(rel)
        assert len(interests) == 1
        assert interests[0]["type"] == "shareholding"
