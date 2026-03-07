"""Tests for the CSV reader."""

import csv
import tempfile
from pathlib import Path

from bods_opencorporates.ingestion.csv_reader import BulkCSVReader


def _write_csv(data: list[dict], filepath: Path) -> None:
    """Helper to write a list of dicts as a CSV file."""
    if not data:
        return
    fieldnames = list(data[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


class TestBulkCSVReaderCompanies:
    def test_read_companies(self, tmp_path):
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
            {
                "company_number": "12345678",
                "jurisdiction_code": "dk",
                "name": "Test ApS",
                "company_type": "Private Limited",
                "incorporation_date": "2020-01-01",
                "dissolution_date": "",
                "current_status": "Active",
            },
        ]
        filepath = tmp_path / "companies.csv"
        _write_csv(csv_data, filepath)

        reader = BulkCSVReader()
        companies = list(reader.read_companies(filepath))

        assert len(companies) == 2
        assert companies[0].company_number == "00445790"
        assert companies[0].name == "TESCO PLC"
        assert companies[0].jurisdiction_code == "gb"
        assert companies[1].jurisdiction_code == "dk"


class TestBulkCSVReaderOfficers:
    def test_read_officers(self, tmp_path):
        csv_data = [
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
                "country_of_residence": "United Kingdom",
                "partial_date_of_birth": "1975-03",
                "occupation": "Executive",
                "type": "Person",
            },
        ]
        filepath = tmp_path / "officers.csv"
        _write_csv(csv_data, filepath)

        reader = BulkCSVReader()
        officers = list(reader.read_officers(filepath))

        assert len(officers) == 1
        assert officers[0].full_name == "John Smith"
        assert officers[0].position == "Director"
        assert officers[0].nationality == "British"


class TestBulkCSVReaderRelationships:
    def test_read_relationships(self, tmp_path):
        csv_data = [
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
        filepath = tmp_path / "relationships.csv"
        _write_csv(csv_data, filepath)

        reader = BulkCSVReader()
        relationships = list(reader.read_relationships(filepath))

        assert len(relationships) == 1
        assert relationships[0].relationship_type == "subsidiary"
        assert relationships[0].percentage_min_share_ownership == 100.0
        assert relationships[0].percentage_max_voting_rights is None
        assert relationships[0].subject_name == "TESCO PLC"
