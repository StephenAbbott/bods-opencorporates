"""Bulk CSV file reader for OpenCorporates data.

OpenCorporates provides bulk data as CSV files with the following types:
    - Companies: company_number, jurisdiction_code, name, etc.
    - Officers: id, company_number, jurisdiction_code, full_name, position, etc.
    - Relationships: relationship_type, subject/object company details, percentages.

This module reads these files and yields typed dataclass instances.
"""

from __future__ import annotations

import csv
import io
import logging
from pathlib import Path
from typing import Iterator

from bods_opencorporates.ingestion.models import OCCompany, OCOfficer, OCRelationship

logger = logging.getLogger(__name__)


class BulkCSVReader:
    """Reads OpenCorporates bulk CSV files and yields typed records.

    All reading is done via streaming iterators to handle multi-GB files
    without loading everything into memory.
    """

    def __init__(self, encoding: str = "utf-8"):
        self.encoding = encoding

    def read_companies(self, filepath: Path | str) -> Iterator[OCCompany]:
        """Read companies from a bulk CSV file.

        Args:
            filepath: Path to the companies CSV file.

        Yields:
            OCCompany instances.
        """
        filepath = Path(filepath)
        logger.info("Reading companies from %s", filepath)
        count = 0

        for row in self._read_csv(filepath):
            try:
                yield OCCompany.from_csv_row(row)
                count += 1
            except (KeyError, ValueError) as e:
                logger.warning(
                    "Skipping invalid company row: %s (error: %s)",
                    row.get("company_number", "unknown"),
                    e,
                )

        logger.info("Read %d companies from %s", count, filepath)

    def read_officers(self, filepath: Path | str) -> Iterator[OCOfficer]:
        """Read officers from a bulk CSV file.

        Args:
            filepath: Path to the officers CSV file.

        Yields:
            OCOfficer instances.
        """
        filepath = Path(filepath)
        logger.info("Reading officers from %s", filepath)
        count = 0

        for row in self._read_csv(filepath):
            try:
                yield OCOfficer.from_csv_row(row)
                count += 1
            except (KeyError, ValueError) as e:
                logger.warning(
                    "Skipping invalid officer row: %s (error: %s)",
                    row.get("id", "unknown"),
                    e,
                )

        logger.info("Read %d officers from %s", count, filepath)

    def read_relationships(self, filepath: Path | str) -> Iterator[OCRelationship]:
        """Read relationships from a bulk CSV file.

        Args:
            filepath: Path to the relationships CSV file.

        Yields:
            OCRelationship instances.
        """
        filepath = Path(filepath)
        logger.info("Reading relationships from %s", filepath)
        count = 0

        for row in self._read_csv(filepath):
            try:
                yield OCRelationship.from_csv_row(row)
                count += 1
            except (KeyError, ValueError) as e:
                logger.warning(
                    "Skipping invalid relationship row: %s (error: %s)",
                    row.get("oc_relationship_identifier", "unknown"),
                    e,
                )

        logger.info("Read %d relationships from %s", count, filepath)

    def _read_csv(self, filepath: Path) -> Iterator[dict]:
        """Stream CSV rows as dicts, handling encoding edge cases.

        Tries UTF-8 first, falls back to UTF-8 with BOM, then Latin-1.
        """
        encodings = [self.encoding, "utf-8-sig", "latin-1"]

        for enc in encodings:
            try:
                with open(filepath, "r", encoding=enc, newline="") as f:
                    # Sniff the dialect
                    sample = f.read(8192)
                    f.seek(0)

                    try:
                        dialect = csv.Sniffer().sniff(sample)
                    except csv.Error:
                        dialect = csv.excel

                    reader = csv.DictReader(f, dialect=dialect)
                    for row in reader:
                        # Strip whitespace from keys and values
                        cleaned = {
                            k.strip(): v.strip() if isinstance(v, str) else v
                            for k, v in row.items()
                            if k is not None
                        }
                        yield cleaned
                    return  # Successfully read the file

            except UnicodeDecodeError:
                logger.debug("Failed to read %s with encoding %s, trying next", filepath, enc)
                continue

        raise ValueError(
            f"Could not read {filepath} with any supported encoding: {encodings}"
        )
