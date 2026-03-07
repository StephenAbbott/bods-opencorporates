"""Publisher configuration for the BODS OpenCorporates pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field

from bods_opencorporates.utils.dates import current_date_iso, current_datetime_iso


@dataclass
class PublisherConfig:
    """Configuration for the BODS publication metadata.

    This is used to populate the publicationDetails and source fields
    on every BODS statement produced by the pipeline.
    """

    publisher_name: str = "BODS OpenCorporates Pipeline"
    publisher_uri: str | None = None
    license_url: str = "https://creativecommons.org/publicdomain/zero/1.0/"
    publication_date: str = field(default_factory=current_date_iso)
    retrieved_at: str | None = field(default_factory=current_datetime_iso)
    output_path: str = "output.json"
    output_format: str = "json"  # "json" or "jsonl"
    api_token: str | None = None

    @classmethod
    def from_cli_args(
        cls,
        publisher_name: str | None = None,
        publisher_uri: str | None = None,
        license_url: str | None = None,
        output: str | None = None,
        output_format: str | None = None,
        api_token: str | None = None,
    ) -> PublisherConfig:
        """Create a PublisherConfig from CLI arguments, using defaults for unset values."""
        return cls(
            publisher_name=publisher_name or cls.publisher_name,
            publisher_uri=publisher_uri,
            license_url=license_url or cls.license_url,
            output_path=output or cls.output_path,
            output_format=output_format or cls.output_format,
            api_token=api_token,
        )
