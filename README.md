# OCS Submission Capsule

[![CI](https://github.com/AllenInstitute/OCS-Submission-Capsule-API/actions/workflows/ci.yml/badge.svg)](https://github.com/AllenInstitute/OCS-Submission-Capsule-API/actions/workflows/ci.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)

## Overview

OCS Submission Capsule reads FASTQ metadata, checks OCS stage status, builds commands, and submits jobs through the `ocs` CLI.

It supports daily runs and backfills. Each run writes a manifest with one row per FASTQ sample. The manifest records command values, submission status, demand IDs, errors, and timestamps.

When OCS reaches the job limit, the capsule waits and checks the limit again before submitting the next command.

The audit queries LIMS for a vendor batch, writes CSV reports for missing fields, and sends a plain-text email. Use the CellFlex LIMS query for an RFX audit.

Add alignment and post-alignment commands in `config.jsonc`. The code reads those command templates at runtime.

## Table of Contents

* [Run it](#run-it)
* [Commands and stages](#commands-and-stages)
* [Workflow](#workflow)
* [Inputs](#inputs)
* [CLI options](#cli-options)
* [Configuration](#configuration)
* [Outputs](#outputs)
* [Environment](#environment)
* [Project layout](#project-layout)
* [Development](#development)
* [Changelog](#changelog)
* [Authors](#authors)
* [Acknowledgments](#acknowledgments)

## Run it

Run these commands from a Python 3.12+ environment with the `ocs` CLI on `PATH`.

1. Install the package:

    ```bash
    uv sync --frozen
    ```

    Or with plain pip:

    ```bash
    pip install -e .
    ```

2. Set required environment variables:

    ```bash
    export RUNNING_JOBS_DB_URL=postgresql://...
    ```

3. Run a dry run first to verify planned commands:

    ```bash
    ocs-submission \
      --modality MTX \
      --batch-name-from-vendor MTX-22068 \
      --dry-run true
    ```

4. If the planned commands look correct, rerun without `--dry-run`:

    ```bash
    ocs-submission \
      --modality MTX \
      --batch-name-from-vendor MTX-22068
    ```

5. To force resubmission of a stage:

    ```bash
    ocs-submission \
      --modality MTX \
      --batch-name-from-vendor MTX-22068 \
      --force-submission alignment
    ```

6. To run with a LIMS audit and email notification:

    ```bash
    ocs-submission \
      --modality RTX \
      --batch-name-from-vendor RTX-34056 \
      --audit true \
      --email BICore@alleninstitute.org
    ```

> **Note:** Requires Python 3.12+ and the `ocs` CLI available on `PATH`.

## Commands and stages

- Check ingest, alignment, and post-alignment status for each FASTQ sample on OCS.
- Load FASTQ metadata from an OCS Tracker export CSV, a vendor batch name, or FASTQ names.
- Create an alignment command only after FASTQ sample ingest is complete.
- Build a post-alignment command only after alignment is complete.
- Skip a FASTQ sample when its library prep has no command.
- Skip a stage when it is complete or already in progress.
- Submit commands through the `ocs` CLI within the configured job limit.
- Save submitted jobs in PostgreSQL so later runs can check their status.
- Run a LIMS audit for a vendor batch when `--audit true` is set.
- Write a JSON manifest with planned commands and submission results.
- Send submission summaries through AWS SES.

## Workflow

For each FASTQ sample, the capsule loads metadata, checks stage status, builds the next command, submits the command or prints it during a dry run, and writes the result to the manifest. When `--audit true` is set, it checks the batch metadata in LIMS and writes missing-data reports.
```
Input (exporter CSV / batch name / FASTQ names)
        │
        ▼
┌─────────────────────────┐
│  Load FASTQ Metadata    │  query_metadata → fastq_records_df
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│  Check Stage Status     │  OCS list results → join on fastq_name
│                         │  DB fallback for align / postalign
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│  Build Job Commands     │  config.jsonc templates → command records
│                         │  align_should_execute / postalign_should_execute
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│  Submit to OCS          │  ocs CLI → demand_id
│  (or dry run)           │  tracker DB write
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│  Write Manifest         │  ocs_job_commands_manifest.json
│  Send Email             │  AWS SES summary
│  Run Audit (optional)   │  LIMS CSV reports + summary email
└─────────────────────────┘
```

## Inputs

Exactly one of the following is required:

### OCS Tracker Export CSV

```bash
ocs-submission \
  --ocs-tracker-exporter /path/to/ocs_tracker_export.csv \
  --modality RTX \
  --dry-run true
```

### Batch name from vendor

```bash
ocs-submission \
  --batch-name-from-vendor MTX-22068 \
  --modality MTX \
  --dry-run true
```

### Fastq names

```bash
ocs-submission \
  --fastq-names NY-MX22068-2 NY-MX22068-3 \
  --modality MTX \
  --dry-run true
```

## CLI options

| Option | Required | Description |
|---|---|---|
| `--modality` | Yes | Workflow modality: `RTX`, `MTX`, or `RFX` |
| `--ocs-tracker-exporter` | No | Path to an OCS Tracker export CSV |
| `--batch-name-from-vendor` | No | Batch Name From Vendor |
| `--fastq-names` | No | One or more FASTQ names |
| `--force-submission` | No | Force `alignment` or `post-alignment` regardless of current status |
| `--email`, `-e` | No | Email for OCS job notifications and run summary emails |
| `--dry-run` | No | `true` or `false` (default `false`) — log commands without executing |
| `--audit` | No | `true` or `false` (default `false`) — run LIMS audit for a batch name from vendor |
| `--batch-processing` | No | `true` or `false` (default `false`) — use FASTQ names for RTX/RFX alignment and post-alignment commands |
| `--config` | No | Path to JSONC config; defaults to included `config.jsonc` |

## Configuration

The capsule reads command templates and status mappings from:

```
src/ocs_submission/config.jsonc
```

Key sections:

| Section | Purpose |
|---|---|
| `references` | Maps organisms and modalities to reference genome names, optionally by library prep |
| `probe_sets_by_organism` | Optional shared probe set per organism, or a mapping by library prep |
| `chemistry_by_library_prep` | Maps library prep names to chemistry strings |
| `workflows` | Alignment and post-alignment command templates for `MTX`, `RTX`, and `RFX` |
| `job_settings` | Submission limits and spacing between job submissions |
| `status_mappings` | Defines which OCS statuses count as complete |

Command templates support placeholders such as `{reference_name}`, `{load_name}`, `{input_name}`, `{input_name_flag}`, `{email}`, `{chemistry}`, `{probe_set}`, and `{execution_vcpus}`. `{input_name}` and `{input_name_flag}` are used together to render either `--load-names <load_name>` or, for RTX/RFX batch processing, `--fastq-names <fastq_name>`.

When alignment or post-alignment is due but a FASTQ sample's library prep has no command, the capsule skips that stage and reports the FASTQ name in the log and summary email.
Missing chemistry and probe-set mappings continue to render as empty command values.

A modality reference can be a single reference name, preserving the existing behavior:

```json
"RTX": "mouse_10x_mm10_genome_star2.7.1a"
```

When library preps for the same organism and modality require different references,
use a `library_preps` mapping. Every submitted library prep must have an entry:

```json
"RFX": {
  "library_preps": {
    "10xV4_FX16": "mouse_10x_mm10-flex-custom-v1_probe-genome_cr9.0.1",
    "10xFXv2": "mouse_10x_grcm39-fx2v01_probe-genome_cr10.0.0"
  }
}
```

## Outputs

| Output | Location | Description |
|---|---|---|
| `ocs_job_commands_manifest.json` | `/results` or current directory | One row per FASTQ with planned commands and execution results |
| `<batch>_<modality>_missing_data.csv` | `/results` or current directory | Missing LIMS data report (when `--audit true`) |
| `<batch>_lims_pull.csv` | `/results` or current directory | Full LIMS pull for the batch (when `--audit true`) |

## Environment

| Variable | Used by | Purpose |
|---|---|---|
| `RUNNING_JOBS_DB_URL` | `running_jobs_db` | PostgreSQL connection URL for the tracker DB |
| `DATABASE_USERNAME` | `audit` | LIMS database user |
| `DATABASE_PASSWORD` | `audit` | LIMS database password |

> Environment variables set during Code Ocean's post-install phase are not automatically available in later capsule runs or terminal sessions. Make sure they are set in the runtime environment.

## Project layout

```
src/ocs_submission/
├── __init__.py
├── __main__.py              # python -m ocs_submission entry point
├── main.py                  # CLI entry (exposed as ocs-submission)
├── config.jsonc             # Workflow templates and status mappings
├── environment.py           # Environment variable accessors
├── stages.py                # Stage enum (ingest / align / postalign)
├── ocs_cli.py               # ocs CLI wrapper, job limits, command submission
├── ocs_command_builder.py   # Build alignment + post-alignment commands
├── fastq_info_fetcher.py    # Load FASTQ records from exporter / batch / names
├── emails.py                # Summary + audit email via AWS SES
├── running_jobs_db.py       # Tracker PostgreSQL helpers
└── audit/
    ├── __init__.py
    ├── audit.py             # LIMS audit (exports run_audit)
    ├── rnaseq_and_multiome_lims_metadata_pull.sql
    └── cellflex_lims_metadata_pull.sql
```

## Development

Install with dev dependencies (ruff, mypy, pytest):

```bash
uv sync --extra dev --frozen
```

Run checks:

```bash
uv run ruff format --check src tests   # formatting
uv run ruff check src tests            # lint
uv run pytest                          # tests
uv run mypy src                        # type check (advisory)
```

Auto-fix formatting and safe lint issues:

```bash
uv run ruff format src tests
uv run ruff check --fix src tests
```

After changing dependencies in `pyproject.toml`, regenerate the lockfile:

```bash
uv lock
```

The test suite covers command-building and config logic and does not require a live OCS connection, database, or SES access.

### Releases

Pushing a `vMAJOR.MINOR.PATCH` tag starts the **Release** workflow (`.github/workflows/release.yml`). The workflow runs the tests and publishes a GitHub release from `CHANGELOG.md`.

To cut a release:

1. Start from the latest `main` and create a release branch:
   ```bash
   git checkout main
   git pull origin main
   git checkout -b release/v0.2.0
   ```
2. On the release branch, bump `version` in `pyproject.toml`:
   ```toml
   version = "0.2.0"
   ```
3. In [CHANGELOG.md](CHANGELOG.md), move the release notes out of
   `## [Unreleased]` and into a dated release section:
   ```md
   ## [0.2.0] - YYYY-MM-DD
   ```
4. Commit the release prep and open a PR into `main`:
   ```bash
   git add pyproject.toml CHANGELOG.md
   git commit -m "chore(release): prepare v0.2.0"
   git push -u origin release/v0.2.0
   ```
5. After the PR merges, pull the updated `main`, tag that merged commit, and
   push the tag:
   ```bash
   git checkout main
   git pull origin main
   git tag -a v0.2.0 -m "v0.2.0"
   git push origin v0.2.0
   ```

Before creating a release, check that the Git tag, package version in
`pyproject.toml`, and version section in `CHANGELOG.md` match. The release check
script is `scripts/release/check_version.py`.

## Authors

* Beagan Nguy — Development

## Acknowledgments

Allen Institute Bioinformatics Core Team
