"""Tests for BODS v0.4 compliance using lib-cove-bods.

These tests validate that the pipeline output conforms to the
official BODS schema and passes programmatic validation checks.

Uses: https://github.com/openownership/lib-cove-bods
"""

from __future__ import annotations

import csv
import json
import tempfile
from pathlib import Path

import pytest

from bods_opencorporates.config import PublisherConfig
from bods_opencorporates.ingestion.models import OCAddress, OCCompany, OCOfficer, OCRelationship
from bods_opencorporates.pipeline import BODSPipeline
from bods_opencorporates.transform.entities import transform_company
from bods_opencorporates.transform.persons import transform_officer_person
from bods_opencorporates.transform.relationships import (
    transform_officer_relationship,
    transform_ownership_relationship,
)
from bods_opencorporates.transform.identifiers import (
    company_record_id,
    officer_record_id,
)

# Try to import lib-cove-bods
try:
    from libcovebods.schema import SchemaBODS
    from libcovebods.jsonschemavalidate import JSONSchemaValidator
    from libcovebods.additionalfields import AdditionalFields
    from libcovebods.data_reader import DataReader
    HAS_LIBCOVEBODS = True
except ImportError:
    HAS_LIBCOVEBODS = False


def _write_csv(data: list[dict], filepath: Path) -> None:
    if not data:
        return
    fieldnames = list(data[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def _write_bods_json(statements: list[dict], filepath: Path) -> None:
    """Write statements to a JSON file for lib-cove-bods DataReader."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(statements, f, indent=2, ensure_ascii=False, default=str)


@pytest.fixture
def compliance_config():
    return PublisherConfig(
        publisher_name="Test BODS Publisher",
        publisher_uri="https://test.example.org",
        license_url="https://creativecommons.org/publicdomain/zero/1.0/",
        publication_date="2024-01-15",
        retrieved_at="2024-01-15T10:00:00Z",
    )


def validate_bods_statements(statements: list[dict], tmp_path: Path) -> list:
    """Validate a list of BODS statements using lib-cove-bods.

    lib-cove-bods requires file-based DataReader objects, so we write
    the statements to a temp JSON file first.

    Returns:
        List of validation errors (empty if valid).
    """
    if not HAS_LIBCOVEBODS:
        pytest.skip("lib-cove-bods not installed")

    # Write statements to a temp file for DataReader
    json_file = tmp_path / "bods_test_data.json"
    _write_bods_json(statements, json_file)

    # Create DataReader and Schema
    data_reader = DataReader(str(json_file))
    schema = SchemaBODS(data_reader)

    # Run JSON schema validation
    validator = JSONSchemaValidator(schema)
    errors = validator.validate(data_reader)

    return errors


@pytest.mark.skipif(not HAS_LIBCOVEBODS, reason="lib-cove-bods not installed")
class TestEntityStatementCompliance:
    """Validate entity statements against BODS schema."""

    def test_full_company_entity(self, compliance_config, tmp_path):
        company = OCCompany(
            company_number="00445790",
            jurisdiction_code="gb",
            name="TESCO PLC",
            company_type="Public Limited Company",
            incorporation_date="1947-11-21",
            registered_address=OCAddress(
                street_address="Tesco House, Shire Park",
                locality="Welwyn Garden City",
                region="Hertfordshire",
                postal_code="AL7 1GA",
                country="United Kingdom",
            ),
        )
        stmt = transform_company(company, compliance_config)
        errors = validate_bods_statements([stmt], tmp_path)

        if errors:
            for e in errors:
                print(f"Schema error: {e}")

        assert len(errors) == 0, f"Schema errors: {errors}"

    def test_minimal_company_entity(self, compliance_config, tmp_path):
        company = OCCompany(
            company_number="12345",
            jurisdiction_code="dk",
            name="Test ApS",
        )
        stmt = transform_company(company, compliance_config)
        errors = validate_bods_statements([stmt], tmp_path)
        assert len(errors) == 0, f"Schema errors: {errors}"


@pytest.mark.skipif(not HAS_LIBCOVEBODS, reason="lib-cove-bods not installed")
class TestPersonStatementCompliance:
    """Validate person statements against BODS schema."""

    def test_full_person(self, compliance_config, tmp_path):
        officer = OCOfficer(
            id="12345",
            company_number="00445790",
            jurisdiction_code="gb",
            full_name="John Smith",
            first_name="John",
            last_name="Smith",
            position="Director",
            start_date="2020-01-15",
            nationality="British",
            partial_date_of_birth="1975-03",
            address=OCAddress(
                street_address="123 Main St",
                locality="London",
                postal_code="SW1A 1AA",
                country="United Kingdom",
            ),
        )
        stmt = transform_officer_person(officer, compliance_config)
        errors = validate_bods_statements([stmt], tmp_path)

        if errors:
            for e in errors:
                print(f"Schema error: {e}")

        assert len(errors) == 0, f"Schema errors: {errors}"

    def test_minimal_person(self, compliance_config, tmp_path):
        officer = OCOfficer(
            id="99",
            company_number="00001111",
            jurisdiction_code="fr",
            full_name="Pierre Dupont",
            position="Administrateur",
        )
        stmt = transform_officer_person(officer, compliance_config)
        errors = validate_bods_statements([stmt], tmp_path)
        assert len(errors) == 0, f"Schema errors: {errors}"


@pytest.mark.skipif(not HAS_LIBCOVEBODS, reason="lib-cove-bods not installed")
class TestRelationshipStatementCompliance:
    """Validate relationship statements against BODS schema."""

    def test_officer_relationship(self, compliance_config, tmp_path):
        officer = OCOfficer(
            id="12345",
            company_number="00445790",
            jurisdiction_code="gb",
            full_name="John Smith",
            position="Director",
            start_date="2020-01-15",
        )
        comp_id = company_record_id("gb", "00445790")
        off_id = officer_record_id("gb", "00445790", "12345")
        stmt = transform_officer_relationship(officer, comp_id, off_id, compliance_config)
        errors = validate_bods_statements([stmt], tmp_path)

        if errors:
            for e in errors:
                print(f"Schema error: {e}")

        assert len(errors) == 0, f"Schema errors: {errors}"

    def test_ownership_relationship(self, compliance_config, tmp_path):
        rel = OCRelationship(
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
        stmt = transform_ownership_relationship(rel, compliance_config)
        errors = validate_bods_statements([stmt], tmp_path)
        assert len(errors) == 0, f"Schema errors: {errors}"


@pytest.mark.skipif(not HAS_LIBCOVEBODS, reason="lib-cove-bods not installed")
class TestFullPipelineCompliance:
    """End-to-end compliance: run pipeline on sample data, validate all output."""

    def test_complete_pipeline_output(self, tmp_path, compliance_config):
        """Process companies, officers, and relationships; validate all output."""
        companies_csv = [
            {
                "company_number": "00445790",
                "jurisdiction_code": "gb",
                "name": "TESCO PLC",
                "company_type": "Public Limited Company",
                "incorporation_date": "1947-11-21",
                "dissolution_date": "",
                "current_status": "Active",
            },
        ]
        officers_csv = [
            {
                "id": "12345",
                "company_number": "00445790",
                "jurisdiction_code": "gb",
                "full_name": "John Smith",
                "first_name": "John",
                "last_name": "Smith",
                "position": "Director",
                "start_date": "2020-01-15",
                "end_date": "",
                "nationality": "British",
                "country_of_residence": "",
                "partial_date_of_birth": "1975-03",
                "occupation": "",
                "type": "Person",
            },
        ]
        relationships_csv = [
            {
                "relationship_type": "subsidiary",
                "subject_company_number": "00445790",
                "subject_jurisdiction_code": "gb",
                "subject_name": "TESCO PLC",
                "object_company_number": "99887766",
                "object_jurisdiction_code": "gb",
                "object_name": "Tesco Stores Ltd",
                "percentage_min_share_ownership": "100.0",
                "percentage_max_share_ownership": "100.0",
                "percentage_min_voting_rights": "",
                "percentage_max_voting_rights": "",
                "number_of_shares": "",
                "start_date": "2000-01-01",
                "end_date": "",
            },
        ]

        comp_file = tmp_path / "companies.csv"
        off_file = tmp_path / "officers.csv"
        rel_file = tmp_path / "relationships.csv"
        output_file = tmp_path / "output.json"

        _write_csv(companies_csv, comp_file)
        _write_csv(officers_csv, off_file)
        _write_csv(relationships_csv, rel_file)

        compliance_config.output_path = str(output_file)
        compliance_config.output_format = "json"

        pipeline = BODSPipeline(compliance_config)
        pipeline.process_companies_csv(comp_file)
        pipeline.process_officers_csv(off_file)
        pipeline.process_relationships_csv(rel_file)
        pipeline.finalize()

        # Read the output
        with open(output_file) as f:
            statements = json.load(f)

        assert len(statements) > 0, "Pipeline produced no output"

        # Validate the full output with lib-cove-bods
        errors = validate_bods_statements(statements, tmp_path)

        if errors:
            for e in errors:
                print(f"Validation error: {e}")

        assert len(errors) == 0, f"BODS compliance errors: {errors}"

        # Verify statement types
        types = [s["recordType"] for s in statements]
        assert "entity" in types
        assert "person" in types
        assert "relationship" in types
