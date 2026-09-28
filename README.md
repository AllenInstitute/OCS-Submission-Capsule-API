# OCS Submission Capsule

[![CI](https://github.com/AllenInstitute/OCS-Submission-Capsule-API/actions/workflows/ci.yml/badge.svg)](https://github.com/AllenInstitute/OCS-Submission-Capsule-API/actions/workflows/ci.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)

## Overview

OCS Submission Capsule reads fastq sample metadata, checks OCS stage status, builds commands, and submits jobs through the `ocs` CLI.

It supports daily runs and backfills. Each run writes a manifest with one row per fastq sample, or one row per sequencing load when `--load-names` is used. The manifest records command values, submission status, demand IDs, errors, and timestamps.

When OCS reaches the job limit, the capsule waits and checks the limit again before submitting the next command.

The audit queries LIMS for a vendor batch, writes CSV reports for missing fields, and sends a plain-text email. RFX audits automatically use the CellFlex LIMS query.

Add alignment and post-alignment commands in `src/ocs_submission/config/config.jsonc`. The code reads those command templates at runtime.

## Table of Contents

* [Setup](#setup)
* [Commands and stages](#commands-and-stages)
* [Workflow](#workflow)
* [Inputs](#inputs)
* [CLI options](#cli-options)
* [Configuration](#configuration)
* [Outputs](#outputs)
* [Environment](#environment)
* [Project layout](#project-layout)
* [Linting and testing](#linting-and-testing)
* [Changelog](CHANGELOG.md)
* [Authors](#authors)
* [Acknowledgments](#acknowledgments)

## Setup

Run these commands from a Python 3.12+ environment with the `ocs` CLI on `PATH`.

1. Install the package:

    ```bash
    uv sync --frozen
    ```

    Or with plain pip:

    ```bash
    pip install -e .
    ```

2. To run a LIMS audit, set the database credentials:

    ```bash
    export DATABASE_USERNAME=...
    export DATABASE_PASSWORD=...
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
> LIMS audits require `--email` because the audit reports are generated as email attachments.

## Commands and stages

- Check ingest, alignment, and post-alignment status for each fastq sample on OCS.
- Load fastq sample metadata from an OCS Tracker export CSV, a vendor batch name, load names, or fastq sample names.
- Create an alignment command only after fastq sample ingest is complete.
- Build a post-alignment command only after alignment is complete.
- For load name inputs, check the modality fastq sample and build one command per load.
- Skip a fastq sample when its library prep has no command.
- Skip a stage when it is complete or already in progress.
- Submit commands through the `ocs` CLI within the configured job limit.
- Run a LIMS audit for a vendor batch when `--audit true` is set.
- Write a JSON manifest with planned commands and submission results.
- Send submission summaries through AWS SES.

## Workflow

For each fastq sample or requested load, the capsule loads metadata, checks stage status, builds the next command, submits the command or prints it during a dry run, and writes the result to the manifest. After a non-dry run it can send a submission summary. When `--audit true` and `--email` are set, it audits each unique vendor batch in LIMS and emails the generated reports.
```
Input (exporter CSV / batch name / load names / fastq sample names)
        │
        ▼
┌─────────────────────────┐
│  Load Sample Metadata   │  query_metadata → fastq_records_df
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│  Check Stage Status     │  gather stage status for requested samples
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│  Build Job Commands     │  config.jsonc + OCS metadata → submission commands
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│  Submit to OCS          │  check the number of running OCS jobs
│  (or dry run)           │  submit through the OCS CLI and extract the demand ID
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│  Write Manifest         │  generate ocs_job_commands_manifest.json
│  Send Email             │  send the submission summary by email
│  Run Audit (optional)   │  generate and email audit reports when requested
└─────────────────────────┘
```

## Inputs

Use one of the following input sources:

### OCS Tracker Export CSV

The export from OCS Tracker is checked for the following headers:

- `Fastq Name`
- `Study Set`
- `Load Name`
- `Library Prep Method`
- `Organism` or `Organism Common Name`
- `Ingest`
- `Alignment`
- `Post Alignment`

Capitalization differences and spaces, underscores, or hyphens are accepted, as are clear minor typos. Ambiguous or
missing headers produce an error that identifies the expected field. `Batch Name From Vendor` is optional; when it is
absent, the capsule looks it up from OCS using each fastq sample name.

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

### Load names

For load names, the capsule retrieves every fastq sample associated with each load, checks status using the fastq sample whose vendor batch matches the requested modality, and creates one command and manifest row per load. For multiome loads, the row combines the GEX and ATAC fastq sample and library-prep names while using the MTX/GEX record for status and command configuration.

```bash
ocs-submission \
  --load-names 3796_A01 3796_A02 \
  --modality MTX \
  --dry-run true
```

### Fastq samples

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
| `--load-names` | No | One or more sequencing load names; creates one command per load |
| `--fastq-names` | No | One or more fastq sample names |
| `--force-submission` | No | Force `alignment` or `post-alignment` regardless of its current status; alignment still requires completed ingest and post-alignment still requires completed alignment |
| `--email`, `-e` | No | Email for OCS job notifications and run summary emails; required to generate and send audit reports |
| `--dry-run` | No | `true` or `false` (default `false`) — log commands without executing |
| `--audit` | No | `true` or `false` (default `false`) — audit each unique vendor batch after submission processing |
| `--batch-processing` | No | `true` or `false` (default `false`) — use fastq sample names for RTX/RFX alignment and post-alignment commands |
| `--config` | No | Path to JSONC config; defaults to included `config.jsonc` |

## Configuration

The capsule reads command templates and status mappings from:

```
src/ocs_submission/config/config.jsonc
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

Command templates support placeholders such as `{reference_name}`, `{load_name}`, `{email}`, `{chemistry}`, `{probe_set}`, and `{execution_vcpus}`. For RTX/RFX batch processing, the command builder replaces `--load-names <load_name>` with `--fastq-names <fastq_name>`.

When alignment or post-alignment is due but a fastq sample's library prep has no command, the capsule skips that stage and reports the fastq sample name in the log and summary email.
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
| `ocs_job_commands_manifest.json` | `/results` or current directory | Planned commands and execution results, with one row per fastq sample or requested load |
| `<batch>_<modality>_missing_data.csv` | `/results` or current directory | Missing LIMS data report (when `--audit true`) |
| `<batch>_lims_pull.csv` | `/results` or current directory | Full LIMS pull for the batch (when `--audit true`) |

## Environment

| Variable | Used by | Purpose |
|---|---|---|
| `DATABASE_USERNAME` | LIMS audit | LIMS database user; required only with `--audit true` |
| `DATABASE_PASSWORD` | LIMS audit | LIMS database password; required only with `--audit true` |

> Environment variables set during Code Ocean's post-install phase are not automatically available in later capsule runs or terminal sessions. Make sure they are set in the runtime environment.

## Project layout

```
src/ocs_submission/
├── __init__.py
├── __main__.py              # python -m ocs_submission entry point
├── main.py                  # CLI entry and workflow coordinator
├── config/                   # JSONC loading and workflow configuration
├── workflow/                 # Shared workflow types, including Stage
├── commands/                 # OCS command construction
├── inputs/                   # Fastq sample discovery and record preparation
├── integrations/             # OCS CLI, email, and environment adapters
└── audit/                    # LIMS audit rules and SQL templates
    ├── __init__.py
    ├── audit.py             # LIMS audit (exports run_audit)
    ├── rnaseq_and_multiome_lims_metadata_pull.sql
    └── cellflex_lims_metadata_pull.sql
```

## Linting and testing

Install `just` once with `uv tool install rust-just`. Each recipe uses the locked development dependencies from `uv.lock`.

- Just command to format files

  ```bash
  just format
  ```

- Just command to lint

  ```bash
  just lint
  ```

- Just command to run tests

  ```bash
  just test
  ```

- Just command to run formatting, then linting, then tests all-in-one

  ```bash
  just validate
  ```

## Authors

* Beagan Nguy — Development
* Anish Chakka — Bioinformatics Manager

## Acknowledgments

Allen Institute Bioinformatics Core Team
