# bods-opencorporates

Transform [OpenCorporates](https://opencorporates.com/) data into [Beneficial Ownership Data Standard (BODS)](https://standard.openownership.org/) v0.4 format.

Part of the [BODS Interoperability Toolkit](https://github.com/StephenAbbott/bods-interoperability-toolkit).

## Overview

This pipeline ingests OpenCorporates data — via the REST API or from bulk CSV exports — and produces BODS v0.4 compliant statements, including:

- **Entity statements** for companies
- **Person statements** for officers and individuals
- **Ownership-or-control statements** linking persons to entities, with interest details

## Installation

```bash
pip install .
```

For development (includes pytest and BODS compliance validation):

```bash
pip install ".[dev]"
```

## Usage

### From the OpenCorporates API

Transform a specific company:

```bash
bods-oc from-api --company gb/00445790 -o output.json
```

Search and transform companies:

```bash
bods-oc from-api --search "Acme Corp" -j gb -o output.json
```

Set your API token via the `OC_API_TOKEN` environment variable or `--api-token` flag.

### From bulk CSV files

```bash
bods-oc from-csv --companies companies.csv --officers officers.csv -o output.jsonl
```

Include relationship data:

```bash
bods-oc from-csv --companies c.csv --officers o.csv --relationships r.csv -o output.jsonl
```

### Options

| Flag | Description |
|------|-------------|
| `--api-token` | OpenCorporates API token (or set `OC_API_TOKEN`) |
| `-c`, `--company` | Company in `jurisdiction/number` format (e.g. `gb/00445790`) |
| `-s`, `--search` | Search query for finding companies |
| `-j`, `--jurisdiction` | Filter by jurisdiction code |
| `-n`, `--max-companies` | Max companies to process in search mode |
| `--companies` | Path to companies CSV file |
| `--officers` | Path to officers CSV file |
| `--relationships` | Path to relationships CSV file |
| `-o`, `--output` | Output file path |
| `-f`, `--format` | Output format: `json` or `jsonl` |
| `--publisher-name` | Publisher name for BODS metadata |
| `-v`, `--verbose` | Enable verbose logging |
| `-q`, `--quiet` | Suppress all output except errors |

## Project Structure

```
src/bods_opencorporates/
├── ingestion/       # API client, CSV reader, and data models
├── transform/       # BODS statement generation (entities, persons, relationships, interests, identifiers)
├── output/          # Statement serialisation (JSON/JSONL)
├── utils/           # Country codes, date handling, statement helpers
├── pipeline.py      # Orchestrates ingestion -> transform -> output
└── cli.py           # Click CLI entry point
```

## Testing

```bash
pytest
```

Tests include BODS schema compliance validation via [libcovebods](https://github.com/openownership/lib-cove-bods).

## License

MIT
