"""Command-line interface for the BODS OpenCorporates pipeline.

Usage:
    # Transform a single company via API
    bods-oc from-api --company gb/00445790 -o output.json

    # Search and transform companies via API
    bods-oc from-api --search "Acme Corp" -j gb -o output.json

    # Transform bulk CSV files
    bods-oc from-csv --companies companies.csv --officers officers.csv -o output.jsonl

    # Transform with relationships
    bods-oc from-csv --companies c.csv --officers o.csv --relationships r.csv -o out.jsonl
"""

from __future__ import annotations

import logging
import sys

import click

from bods_opencorporates.config import PublisherConfig
from bods_opencorporates.pipeline import BODSPipeline


@click.group()
@click.option(
    "--verbose", "-v",
    is_flag=True,
    default=False,
    help="Enable verbose logging.",
)
@click.option(
    "--quiet", "-q",
    is_flag=True,
    default=False,
    help="Suppress all output except errors.",
)
def main(verbose: bool, quiet: bool) -> None:
    """Transform OpenCorporates data into BODS v0.4 format."""
    level = logging.WARNING
    if verbose:
        level = logging.DEBUG
    elif not quiet:
        level = logging.INFO

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        stream=sys.stderr,
    )


@main.command("from-api")
@click.option(
    "--api-token",
    envvar="OC_API_TOKEN",
    help="OpenCorporates API token. Can also be set via OC_API_TOKEN env var.",
)
@click.option(
    "--company", "-c",
    help="Specific company to transform: jurisdiction/number (e.g., gb/00445790).",
)
@click.option(
    "--search", "-s",
    help="Search query to find and transform companies.",
)
@click.option(
    "--jurisdiction", "-j",
    help="Filter by jurisdiction code (e.g., gb, us_de).",
)
@click.option(
    "--max-companies", "-n",
    type=int,
    default=None,
    help="Maximum number of companies to process (for search mode).",
)
@click.option(
    "--output", "-o",
    type=click.Path(),
    default="output.json",
    help="Output file path. Use '-' for stdout.",
)
@click.option(
    "--format", "-f",
    "output_format",
    type=click.Choice(["json", "jsonl"]),
    default="json",
    help="Output format.",
)
@click.option(
    "--publisher-name",
    default="BODS OpenCorporates Pipeline",
    help="Publisher name for BODS metadata.",
)
def from_api(
    api_token: str | None,
    company: str | None,
    search: str | None,
    jurisdiction: str | None,
    max_companies: int | None,
    output: str,
    output_format: str,
    publisher_name: str,
) -> None:
    """Transform OpenCorporates data to BODS via the REST API."""
    if not company and not search:
        raise click.UsageError("Either --company or --search must be specified.")

    config = PublisherConfig(
        publisher_name=publisher_name,
        output_path=output,
        output_format=output_format,
        api_token=api_token,
    )

    pipeline = BODSPipeline(config)

    try:
        if company:
            # Single company mode
            parts = company.split("/", 1)
            if len(parts) != 2:
                raise click.UsageError(
                    "Company must be in format: jurisdiction/number (e.g., gb/00445790)"
                )
            jur, num = parts
            pipeline.process_company_from_api(jur, num)
        elif search:
            # Search mode
            pipeline.process_search_from_api(
                search,
                jurisdiction=jurisdiction,
                max_companies=max_companies,
            )

        pipeline.finalize()

    except Exception as e:
        logging.getLogger(__name__).error("Pipeline error: %s", e)
        raise click.ClickException(str(e))


@main.command("from-csv")
@click.option(
    "--companies",
    type=click.Path(exists=True),
    help="Path to the companies CSV file.",
)
@click.option(
    "--officers",
    type=click.Path(exists=True),
    help="Path to the officers CSV file.",
)
@click.option(
    "--relationships",
    type=click.Path(exists=True),
    help="Path to the relationships CSV file.",
)
@click.option(
    "--output", "-o",
    type=click.Path(),
    default="output.jsonl",
    help="Output file path. Use '-' for stdout.",
)
@click.option(
    "--format", "-f",
    "output_format",
    type=click.Choice(["json", "jsonl"]),
    default="jsonl",
    help="Output format (default: jsonl for bulk data).",
)
@click.option(
    "--publisher-name",
    default="BODS OpenCorporates Pipeline",
    help="Publisher name for BODS metadata.",
)
def from_csv(
    companies: str | None,
    officers: str | None,
    relationships: str | None,
    output: str,
    output_format: str,
    publisher_name: str,
) -> None:
    """Transform OpenCorporates bulk CSV data to BODS format."""
    if not any([companies, officers, relationships]):
        raise click.UsageError(
            "At least one of --companies, --officers, or --relationships must be specified."
        )

    config = PublisherConfig(
        publisher_name=publisher_name,
        output_path=output,
        output_format=output_format,
    )

    pipeline = BODSPipeline(config)

    try:
        # Process in order: companies first (entity statements),
        # then officers (person + relationship statements),
        # then relationships (ownership relationship statements).
        if companies:
            pipeline.process_companies_csv(companies)

        if officers:
            pipeline.process_officers_csv(officers)

        if relationships:
            pipeline.process_relationships_csv(relationships)

        pipeline.finalize()

    except Exception as e:
        logging.getLogger(__name__).error("Pipeline error: %s", e)
        raise click.ClickException(str(e))


if __name__ == "__main__":
    main()
