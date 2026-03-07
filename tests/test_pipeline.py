"""Tests for the pipeline orchestrator."""

import csv
import json
import tempfile
from pathlib import Path

from bods_opencorporates.config import PublisherConfig
from bods_opencorporates.pipeline import BODSPipeline


def _write_csv(data: list[dict], filepath: Path) -> None:
    """Helper to write a list of dicts as a CSV file."""
    if not data:
        return
    fieldnames = list(data[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


class TestPipelineCSV:
    def test_companies_csv(self, tmp_path):
        # Write sample companies CSV
        csv_data = [
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
        companies_file = tmp_path / "companies.csv"
        _write_csv(csv_data, companies_file)

        output_file = tmp_path / "output.json"
        config = PublisherConfig(
            publisher_name="Test",
            output_path=str(output_file),
            output_format="json",
            publication_date="2024-01-15",
        )

        pipeline = BODSPipeline(config)
        count = pipeline.process_companies_csv(companies_file)
        pipeline.finalize()

        assert count == 1

        # Read and validate output
        with open(output_file) as f:
            statements = json.load(f)

        assert len(statements) == 1
        assert statements[0]["recordType"] == "entity"
        assert statements[0]["recordDetails"]["name"] == "TESCO PLC"

    def test_officers_csv(self, tmp_path):
        csv_data = [
            {
                "id": "12345",
                "company_number": "00445790",
                "jurisdiction_code": "gb",
                "full_name": "John Smith",
                "position": "Director",
                "start_date": "2020-01-15",
                "end_date": "",
                "nationality": "British",
                "type": "Person",
            },
        ]
        officers_file = tmp_path / "officers.csv"
        _write_csv(csv_data, officers_file)

        output_file = tmp_path / "output.json"
        config = PublisherConfig(
            publisher_name="Test",
            output_path=str(output_file),
            output_format="json",
            publication_date="2024-01-15",
        )

        pipeline = BODSPipeline(config)
        count = pipeline.process_officers_csv(officers_file)
        pipeline.finalize()

        # Should produce person statement + relationship statement
        assert count == 2

        with open(output_file) as f:
            statements = json.load(f)

        types = {s["recordType"] for s in statements}
        assert "person" in types
        assert "relationship" in types

    def test_relationships_csv(self, tmp_path):
        csv_data = [
            {
                "relationship_type": "subsidiary",
                "subject_company_number": "00445790",
                "subject_jurisdiction_code": "gb",
                "subject_name": "Parent Co",
                "object_company_number": "99887766",
                "object_jurisdiction_code": "gb",
                "object_name": "Child Co",
                "percentage_min_share_ownership": "100.0",
                "percentage_max_share_ownership": "100.0",
                "percentage_min_voting_rights": "",
                "percentage_max_voting_rights": "",
                "number_of_shares": "",
                "start_date": "2000-01-01",
                "end_date": "",
            },
        ]
        rel_file = tmp_path / "relationships.csv"
        _write_csv(csv_data, rel_file)

        output_file = tmp_path / "output.json"
        config = PublisherConfig(
            publisher_name="Test",
            output_path=str(output_file),
            output_format="json",
            publication_date="2024-01-15",
        )

        pipeline = BODSPipeline(config)
        count = pipeline.process_relationships_csv(rel_file)
        pipeline.finalize()

        # Should produce 2 entity statements + 1 relationship statement
        assert count == 3

        with open(output_file) as f:
            statements = json.load(f)

        entity_stmts = [s for s in statements if s["recordType"] == "entity"]
        rel_stmts = [s for s in statements if s["recordType"] == "relationship"]
        assert len(entity_stmts) == 2
        assert len(rel_stmts) == 1

    def test_entity_deduplication(self, tmp_path):
        """Companies should only be emitted once even when referenced multiple times."""
        csv_data = [
            {
                "relationship_type": "subsidiary",
                "subject_company_number": "00001111",
                "subject_jurisdiction_code": "gb",
                "subject_name": "Parent",
                "object_company_number": "00002222",
                "object_jurisdiction_code": "gb",
                "object_name": "Child1",
                "percentage_min_share_ownership": "100.0",
                "percentage_max_share_ownership": "100.0",
                "percentage_min_voting_rights": "",
                "percentage_max_voting_rights": "",
                "number_of_shares": "",
                "start_date": "",
                "end_date": "",
            },
            {
                "relationship_type": "subsidiary",
                "subject_company_number": "00001111",
                "subject_jurisdiction_code": "gb",
                "subject_name": "Parent",
                "object_company_number": "00003333",
                "object_jurisdiction_code": "gb",
                "object_name": "Child2",
                "percentage_min_share_ownership": "100.0",
                "percentage_max_share_ownership": "100.0",
                "percentage_min_voting_rights": "",
                "percentage_max_voting_rights": "",
                "number_of_shares": "",
                "start_date": "",
                "end_date": "",
            },
        ]
        rel_file = tmp_path / "relationships.csv"
        _write_csv(csv_data, rel_file)

        output_file = tmp_path / "output.json"
        config = PublisherConfig(
            publisher_name="Test",
            output_path=str(output_file),
            output_format="json",
            publication_date="2024-01-15",
        )

        pipeline = BODSPipeline(config)
        pipeline.process_relationships_csv(rel_file)
        pipeline.finalize()

        with open(output_file) as f:
            statements = json.load(f)

        entity_stmts = [s for s in statements if s["recordType"] == "entity"]
        # Parent should appear once, Child1 and Child2 each once = 3 entities
        assert len(entity_stmts) == 3

    def test_jsonl_output(self, tmp_path):
        csv_data = [
            {
                "company_number": "00445790",
                "jurisdiction_code": "gb",
                "name": "Test Co",
                "company_type": "",
                "incorporation_date": "",
                "dissolution_date": "",
                "current_status": "",
            },
        ]
        companies_file = tmp_path / "companies.csv"
        _write_csv(csv_data, companies_file)

        output_file = tmp_path / "output.jsonl"
        config = PublisherConfig(
            publisher_name="Test",
            output_path=str(output_file),
            output_format="jsonl",
            publication_date="2024-01-15",
        )

        pipeline = BODSPipeline(config)
        pipeline.process_companies_csv(companies_file)
        pipeline.finalize()

        # JSONL: each line should be valid JSON
        with open(output_file) as f:
            lines = f.readlines()

        assert len(lines) == 1
        stmt = json.loads(lines[0])
        assert stmt["recordType"] == "entity"


class TestPipelineFullIntegration:
    def test_companies_and_officers(self, tmp_path):
        """Test processing companies then officers together."""
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
                "position": "Director",
                "start_date": "2020-01-15",
                "end_date": "",
                "nationality": "British",
                "type": "Person",
            },
            {
                "id": "67890",
                "company_number": "00445790",
                "jurisdiction_code": "gb",
                "full_name": "Jane Doe",
                "position": "Company Secretary",
                "start_date": "2019-06-01",
                "end_date": "",
                "nationality": "Irish",
                "type": "Person",
            },
        ]

        comp_file = tmp_path / "companies.csv"
        off_file = tmp_path / "officers.csv"
        _write_csv(companies_csv, comp_file)
        _write_csv(officers_csv, off_file)

        output_file = tmp_path / "output.json"
        config = PublisherConfig(
            publisher_name="Test",
            output_path=str(output_file),
            output_format="json",
            publication_date="2024-01-15",
        )

        pipeline = BODSPipeline(config)
        pipeline.process_companies_csv(comp_file)
        pipeline.process_officers_csv(off_file)
        pipeline.finalize()

        with open(output_file) as f:
            statements = json.load(f)

        # 1 entity + 2 persons + 2 relationships = 5 statements
        assert len(statements) == 5

        types = [s["recordType"] for s in statements]
        assert types.count("entity") == 1
        assert types.count("person") == 2
        assert types.count("relationship") == 2

        # Verify cross-references: relationships should reference the entity
        entity_record_id = statements[0]["recordId"]
        for rel in [s for s in statements if s["recordType"] == "relationship"]:
            assert rel["recordDetails"]["subject"] == entity_record_id

        # Verify publication details on all statements
        for stmt in statements:
            assert stmt["publicationDetails"]["bodsVersion"] == "0.4"
            assert stmt["publicationDetails"]["publisher"]["name"] == "Test"
