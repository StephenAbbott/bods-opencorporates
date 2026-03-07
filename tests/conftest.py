"""Shared test fixtures for the BODS OpenCorporates pipeline."""

from __future__ import annotations

import pytest

from bods_opencorporates.config import PublisherConfig
from bods_opencorporates.ingestion.models import OCAddress, OCCompany, OCOfficer, OCRelationship


@pytest.fixture
def test_config() -> PublisherConfig:
    """Standard test configuration."""
    return PublisherConfig(
        publisher_name="Test Publisher",
        publisher_uri="https://test.example.org",
        license_url="https://creativecommons.org/publicdomain/zero/1.0/",
        publication_date="2024-01-15",
        retrieved_at="2024-01-15T10:00:00Z",
        output_path="test_output.json",
        output_format="json",
    )


@pytest.fixture
def sample_address() -> OCAddress:
    """A sample OpenCorporates address."""
    return OCAddress(
        street_address="123 Main Street",
        locality="London",
        region="Greater London",
        postal_code="SW1A 1AA",
        country="United Kingdom",
    )


@pytest.fixture
def sample_company(sample_address: OCAddress) -> OCCompany:
    """A sample UK company with full details."""
    return OCCompany(
        company_number="00445790",
        jurisdiction_code="gb",
        name="TESCO PLC",
        company_type="Public Limited Company",
        incorporation_date="1947-11-21",
        dissolution_date=None,
        current_status="Active",
        registered_address=sample_address,
        previous_names=["TESCO STORES (HOLDINGS) LIMITED"],
        branch=None,
        home_jurisdiction_code=None,
        home_company_number=None,
    )


@pytest.fixture
def sample_company_minimal() -> OCCompany:
    """A company with minimal data."""
    return OCCompany(
        company_number="12345678",
        jurisdiction_code="dk",
        name="Test Danish Company ApS",
    )


@pytest.fixture
def sample_officer_person() -> OCOfficer:
    """A sample natural person officer."""
    return OCOfficer(
        id="12345",
        company_number="00445790",
        jurisdiction_code="gb",
        full_name="John Smith",
        first_name="John",
        last_name="Smith",
        position="Director",
        start_date="2020-01-15",
        end_date=None,
        nationality="British",
        country_of_residence="United Kingdom",
        partial_date_of_birth="1975-03",
        occupation="Business Executive",
        officer_type="Person",
        address=OCAddress(
            street_address="456 High Street",
            locality="London",
            postal_code="EC1A 1BB",
            country="United Kingdom",
        ),
        person_uid=None,
    )


@pytest.fixture
def sample_officer_corporate() -> OCOfficer:
    """A sample corporate officer."""
    return OCOfficer(
        id="67890",
        company_number="00445790",
        jurisdiction_code="gb",
        full_name="ACME Holdings Ltd",
        position="Corporate Director",
        officer_type="Company",
    )


@pytest.fixture
def sample_officer_secretary() -> OCOfficer:
    """A sample company secretary."""
    return OCOfficer(
        id="11111",
        company_number="00445790",
        jurisdiction_code="gb",
        full_name="Jane Doe",
        first_name="Jane",
        last_name="Doe",
        position="Company Secretary",
        start_date="2019-06-01",
        nationality="Irish",
    )


@pytest.fixture
def sample_relationship_subsidiary() -> OCRelationship:
    """A sample subsidiary relationship."""
    return OCRelationship(
        relationship_type="subsidiary",
        subject_company_number="00445790",
        subject_jurisdiction_code="gb",
        subject_name="TESCO PLC",
        object_company_number="99887766",
        object_jurisdiction_code="gb",
        object_name="Tesco Stores Ltd",
        percentage_min_share_ownership=100.0,
        percentage_max_share_ownership=100.0,
        start_date="2000-01-01",
    )


@pytest.fixture
def sample_relationship_share() -> OCRelationship:
    """A sample share parcel relationship."""
    return OCRelationship(
        relationship_type="share_parcel",
        subject_company_number="55667788",
        subject_jurisdiction_code="us_de",
        subject_name="Tech Corp Inc",
        object_company_number="11223344",
        object_jurisdiction_code="us_ca",
        object_name="Venture Fund LLC",
        percentage_min_share_ownership=25.0,
        percentage_max_share_ownership=50.0,
        percentage_min_voting_rights=25.0,
        percentage_max_voting_rights=50.0,
        number_of_shares=1000000,
        start_date="2022-03-15",
    )
