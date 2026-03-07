"""Pipeline orchestrator for transforming OpenCorporates data to BODS.

Ties together ingestion, transformation, and output into a coherent
workflow. Supports both API and bulk CSV input modes.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator

from bods_opencorporates.config import PublisherConfig
from bods_opencorporates.ingestion.api_client import OpenCorporatesAPI
from bods_opencorporates.ingestion.csv_reader import BulkCSVReader
from bods_opencorporates.ingestion.models import OCCompany, OCOfficer, OCRelationship
from bods_opencorporates.output.writer import BODSWriter
from bods_opencorporates.transform.entities import (
    build_minimal_entity,
    transform_company,
    transform_corporate_officer_entity,
)
from bods_opencorporates.transform.identifiers import (
    company_record_id,
    officer_record_id,
)
from bods_opencorporates.transform.persons import transform_officer_person
from bods_opencorporates.transform.relationships import (
    transform_officer_relationship,
    transform_ownership_relationship,
)

logger = logging.getLogger(__name__)


class BODSPipeline:
    """Orchestrates the transformation of OpenCorporates data to BODS format.

    Manages entity deduplication, statement generation, and output writing.

    Usage (API mode):
        config = PublisherConfig(api_token="...", output_path="output.json")
        pipeline = BODSPipeline(config)
        pipeline.process_company_from_api("gb", "00445790")
        pipeline.finalize()

    Usage (CSV mode):
        config = PublisherConfig(output_path="output.jsonl", output_format="jsonl")
        pipeline = BODSPipeline(config)
        pipeline.process_companies_csv("companies.csv")
        pipeline.process_officers_csv("officers.csv")
        pipeline.process_relationships_csv("relationships.csv")
        pipeline.finalize()
    """

    def __init__(self, config: PublisherConfig):
        self.config = config
        self.writer = BODSWriter(config.output_path, config.output_format)
        self._emitted_record_ids: set[str] = set()
        self._api: OpenCorporatesAPI | None = None

    @property
    def api(self) -> OpenCorporatesAPI:
        """Lazily initialize the API client."""
        if self._api is None:
            self._api = OpenCorporatesAPI(api_token=self.config.api_token)
        return self._api

    @property
    def statement_count(self) -> int:
        """Number of statements written so far."""
        return self.writer._count

    # ── API Mode ──────────────────────────────────────────────────────

    def process_company_from_api(
        self,
        jurisdiction: str,
        company_number: str,
    ) -> list[dict]:
        """Fetch and transform a single company and its officers via API.

        Args:
            jurisdiction: OpenCorporates jurisdiction code.
            company_number: Company registration number.

        Returns:
            List of BODS statements generated.
        """
        logger.info("Processing company %s/%s via API", jurisdiction, company_number)

        # Fetch company
        company = self.api.get_company(jurisdiction, company_number)
        statements = self._process_company_with_officers(company)
        self.writer.write_statements(statements)
        return statements

    def process_search_from_api(
        self,
        query: str,
        jurisdiction: str | None = None,
        max_companies: int | None = None,
    ) -> int:
        """Search for companies and transform them with their officers.

        Args:
            query: Search query string.
            jurisdiction: Optional jurisdiction filter.
            max_companies: Maximum number of companies to process.

        Returns:
            Total number of BODS statements generated.
        """
        logger.info("Searching companies: query='%s', jurisdiction=%s", query, jurisdiction)
        count = 0
        total_statements = 0

        for company in self.api.search_companies(query, jurisdiction):
            if max_companies and count >= max_companies:
                break

            try:
                statements = self._process_company_with_officers(company)
                self.writer.write_statements(statements)
                total_statements += len(statements)
                count += 1
                logger.info(
                    "Processed %d/%s companies (%d statements)",
                    count,
                    max_companies or "unlimited",
                    total_statements,
                )
            except Exception as e:
                logger.error(
                    "Error processing company %s/%s: %s",
                    company.jurisdiction_code,
                    company.company_number,
                    e,
                )

        return total_statements

    # ── CSV Mode ──────────────────────────────────────────────────────

    def process_companies_csv(self, filepath: Path | str) -> int:
        """Process a companies CSV file, emitting entity statements.

        Args:
            filepath: Path to the companies CSV file.

        Returns:
            Number of statements generated.
        """
        reader = BulkCSVReader()
        count = 0

        for company in reader.read_companies(filepath):
            record_id = company_record_id(
                company.jurisdiction_code, company.company_number
            )
            if record_id not in self._emitted_record_ids:
                stmt = transform_company(company, self.config)
                self.writer.write_statements([stmt])
                self._emitted_record_ids.add(record_id)
                count += 1

                if count % 10000 == 0:
                    logger.info("Processed %d company entities", count)

        logger.info("Generated %d entity statements from companies CSV", count)
        return count

    def process_officers_csv(self, filepath: Path | str) -> int:
        """Process an officers CSV file, emitting person and relationship statements.

        Args:
            filepath: Path to the officers CSV file.

        Returns:
            Number of statements generated.
        """
        reader = BulkCSVReader()
        count = 0

        for officer in reader.read_officers(filepath):
            statements = self._transform_officer(officer)
            if statements:
                self.writer.write_statements(statements)
                count += len(statements)

            if count % 10000 == 0 and count > 0:
                logger.info("Processed %d officer statements", count)

        logger.info("Generated %d statements from officers CSV", count)
        return count

    def process_relationships_csv(self, filepath: Path | str) -> int:
        """Process a relationships CSV file, emitting entity and relationship statements.

        For each relationship, ensures both subject and object entities exist
        (creating minimal entity statements if needed), then creates the
        relationship statement.

        Args:
            filepath: Path to the relationships CSV file.

        Returns:
            Number of statements generated.
        """
        reader = BulkCSVReader()
        count = 0

        for rel in reader.read_relationships(filepath):
            statements = self._transform_relationship(rel)
            if statements:
                self.writer.write_statements(statements)
                count += len(statements)

            if count % 10000 == 0 and count > 0:
                logger.info("Processed %d relationship statements", count)

        logger.info("Generated %d statements from relationships CSV", count)
        return count

    # ── Finalization ──────────────────────────────────────────────────

    def finalize(self) -> None:
        """Finalize the output (flush buffers, close files)."""
        self.writer.finalize()
        logger.info(
            "Pipeline complete: %d total statements, %d unique entities tracked",
            self.writer._count,
            len(self._emitted_record_ids),
        )

    # ── Internal ──────────────────────────────────────────────────────

    def _process_company_with_officers(self, company: OCCompany) -> list[dict]:
        """Transform a company and its officers into BODS statements.

        Returns:
            List of BODS statements (entity + person + relationship).
        """
        statements: list[dict] = []

        # 1. Entity statement for the company
        comp_rec_id = company_record_id(
            company.jurisdiction_code, company.company_number
        )
        if comp_rec_id not in self._emitted_record_ids:
            entity_stmt = transform_company(company, self.config)
            statements.append(entity_stmt)
            self._emitted_record_ids.add(comp_rec_id)

        # 2. Officers and their relationships
        try:
            officers = self.api.get_officers(
                company.jurisdiction_code, company.company_number
            )
            for officer in officers:
                officer_stmts = self._transform_officer(officer)
                statements.extend(officer_stmts)
        except Exception as e:
            logger.warning(
                "Could not fetch officers for %s/%s: %s",
                company.jurisdiction_code,
                company.company_number,
                e,
            )

        return statements

    def _transform_officer(self, officer: OCOfficer) -> list[dict]:
        """Transform an officer into person/entity + relationship statements."""
        statements: list[dict] = []

        comp_rec_id = company_record_id(
            officer.jurisdiction_code, officer.company_number
        )

        if officer.officer_type and officer.officer_type.lower() == "company":
            # Corporate officer → entity statement
            corp_entity = transform_corporate_officer_entity(officer, self.config)
            corp_rec_id = corp_entity["recordId"]
            if corp_rec_id not in self._emitted_record_ids:
                statements.append(corp_entity)
                self._emitted_record_ids.add(corp_rec_id)
            off_rec_id = corp_rec_id
        else:
            # Natural person → person statement
            person_stmt = transform_officer_person(officer, self.config)
            off_rec_id = person_stmt["recordId"]
            # Person statements are not deduplicated by default
            # (unless person_uid is used in record ID generation)
            if off_rec_id not in self._emitted_record_ids:
                statements.append(person_stmt)
                self._emitted_record_ids.add(off_rec_id)

        # Relationship statement
        rel_stmt = transform_officer_relationship(
            officer, comp_rec_id, off_rec_id, self.config
        )
        statements.append(rel_stmt)

        return statements

    def _transform_relationship(self, rel: OCRelationship) -> list[dict]:
        """Transform an ownership relationship, ensuring referenced entities exist."""
        statements: list[dict] = []

        # Ensure subject entity exists
        subject_rec_id = company_record_id(
            rel.subject_jurisdiction_code, rel.subject_company_number
        )
        if subject_rec_id not in self._emitted_record_ids:
            statements.append(
                build_minimal_entity(
                    rel.subject_jurisdiction_code,
                    rel.subject_company_number,
                    rel.subject_name,
                    self.config,
                )
            )
            self._emitted_record_ids.add(subject_rec_id)

        # Ensure object entity exists
        object_rec_id = company_record_id(
            rel.object_jurisdiction_code, rel.object_company_number
        )
        if object_rec_id not in self._emitted_record_ids:
            statements.append(
                build_minimal_entity(
                    rel.object_jurisdiction_code,
                    rel.object_company_number,
                    rel.object_name,
                    self.config,
                )
            )
            self._emitted_record_ids.add(object_rec_id)

        # Relationship statement
        statements.append(
            transform_ownership_relationship(rel, self.config)
        )

        return statements
