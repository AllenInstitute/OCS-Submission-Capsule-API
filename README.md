# OCS Submission Capsule

[![CI](https://github.com/AllenInstitute/OCS-Submission-Capsule-API/actions/workflows/ci.yml/badge.svg)](https://github.com/AllenInstitute/OCS-Submission-Capsule-API/actions/workflows/ci.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![Release](https://img.shields.io/github/v/release/AllenInstitute/OCS-Submission-Capsule-API?label=release&color=informational)](https://github.com/AllenInstitute/OCS-Submission-Capsule-API/releases/latest)

## Overview

The purpose of this codebase is to provide bioinformatics analysts with a user-friendly interface, built on Code Ocean, for submitting and managing ocs jobs.

Previously, analysts needed to manually monitor OCS to determine when a batch or FASTQ sample was ready for the next processing stage. For example, checking whether the ingest stage had finished before submitting an alignment jobs. This codebase automates that process by identifying fastq samples that are ready and automatically submitting the next stage of the workflow.

The system is designed to scale efficiently, supporting hundreds to thousands of jobs. When submission limits are reached, jobs are automatically placed in a queue and submitted as capacity becomes available on OCS.

In addition to daily processing, this codebase simplifies backfill workflows for historical FASTQ samples that need to be reprocessed through OCS. Previously, backfills were executed using custom Bash scripts, making it difficult to track configurations, execution history, and reproducibility. This codebase addresses those challenges by generating a job manifest for every run. The manifest records submitted jobs, failed jobs, and all parameters used to construct each job command, ensuring complete traceability and reproducibility of OCS submissions.

There is also an Audit feature that queries the LIMS database for a given batch name from vendor. It checks that all required metadata fields are present, writes CSV reports for any failed flags, and emails a plain-text summary.

The codebase is lastly highly configurable. New alignment and post-alignment workflows can be added through a centralized configuration file, enabling the system to adapt to evolving pipeline requirements without requiring significant code changes.

## Table of Contents

* [Getting started](#getting-started)
* [What it does](#what-it-does)
* [How it works](#how-it-works)
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

## Getting started

Follow these steps to run the OCS Submission Capsule:

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

## What it does

- Checks ingest, alignment, and post-alignment status for each Fastq sample on OCS.
- Loads FASTQ metadata from an OCS Tracker exporter CSV, a batch name from vendor, or list of fastq names.
- Builds alignment and post-alignment OCS commands from `config.jsonc` templates.
- Skips any work that is already complete or currently in progress.
- Submits jobs through the `ocs` CLI, respecting a configurable job limit.
- Tracks submitted jobs in PostgreSQL so in-flight jobs can be re-checked on later runs.
- Optionally runs a LIMS audit for every batch name from vendor.
- Writes a JSON manifest of all planned and attempted commands.
- Sends summary emails for successful and failed submissions via AWS SES.

## How it works

The capsule follows a linear pipeline: it loads FASTQ samples metadata, checks where each sample stands in the ingest → align → postalign pipeline, builds the appropriate OCS commands, and can submit progress a fastq sample into the next stage in the pipeline. When `--audit true` is passed, it also queries the LIMS database to verify that sample metadata is complete and flags any missing fields before proceeding.
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
| `--fastq-names` | No | One or more Fastq Names |
| `--force-submission` | No | Force `alignment` or `post-alignment` regardless of current status |
| `--email`, `-e` | No | Email for OCS job notifications and run summary emails |
| `--dry-run` | No | `true` or `false` (default `false`) — log commands without executing |
| `--audit` | No | `true` or `false` (default `false`) — run LIMS audit for a batch name from vendor |
| `--config` | No | Path to JSONC config; defaults to included `config.jsonc` |

## Configuration

The capsule reads workflow templates and status mappings from:

```
src/ocs_submission/config.jsonc
```

Key sections:

| Section | Purpose |
|---|---|
| `references` | Maps organisms and modalities to reference genome names |
| `probe_sets_by_organism` | Optional probe-set mapping for supported organism/library-prep combinations |
| `chemistry_by_library_prep` | Maps library prep names to chemistry strings |
| `workflows` | Alignment and post-alignment command templates for `MTX`, `RTX`, and `RFX` |
| `job_settings` | Submission limits and spacing between job submissions |
| `status_mappings` | Defines which OCS statuses count as complete |

Command templates support placeholders such as `{reference_name}`, `{load_name}`, `{email}`, `{chemistry}`, `{probe_set}`, and `{execution_vcpus}`.

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
    ├── lims_mtx_ocs.sql
    └── lims_rtx_ocs.sql
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

Releases are tag-driven. Pushing a `vMAJOR.MINOR.PATCH` tag triggers the **Release** workflow (`.github/workflows/release.yml`), which verifies the tag, runs the tests, and publishes a GitHub release with notes taken from `CHANGELOG.md`.

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

The workflow fails the release if the tag does not point at a commit on `main`,
does not match `pyproject.toml`, or has no matching `CHANGELOG.md` section
(`scripts/check_release_version.py`). That keeps the release tag, package
version, changelog, and published GitHub release in sync.

## Authors

* Beagan Nguy — Development

## Acknowledgments

Allen Institute Bioinformatics Core Team
