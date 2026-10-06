# OCS Submission Capsule

[![CI](https://github.com/AllenInstitute/OCS-Submission-Capsule-API/actions/workflows/ci.yml/badge.svg)](https://github.com/AllenInstitute/OCS-Submission-Capsule-API/actions/workflows/ci.yml)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)

## Overview

OCS Submission Capsule reads fastq sample metadata, checks OCS stage status, builds commands, and submits jobs through the `ocs` CLI.

It supports Batch Processing and Backlog or Resequencing Runs. The manifest has one row per fastq sample or, for load name inputs, per load. Each row records commands, submission status, demand IDs, errors, and timestamps.

When OCS reaches the job limit, the capsule waits and checks the limit again before submitting the next command.

The audit queries LIMS using the batch name from vendor, writes CSV reports for missing fields, and sends a plain-text email. RFX audits automatically use the CellFlex LIMS query.

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

6. To submit batch alignments with a LIMS audit and email notification:

    ```bash
    ocs-submission \
      --modality RTX \
      --batch-name-from-vendor RTX-34056 \
      --email BICore@alleninstitute.org
    ```

> **Note:** Requires Python 3.12+ and the `ocs` CLI available on `PATH`.
> LIMS audits require `--email` because the audit reports are generated as email attachments.

## Commands and stages

- Check ingest, alignment, and post-alignment status for each fastq sample on OCS.
- Load fastq sample metadata from the export file from OCS Tracker, a batch name from vendor, load names, or fastq sample names.
- Create an alignment command only after fastq sample ingest is complete.
- Build a post-alignment command only after alignment is complete.
- For load name inputs, check status using the fastq sample whose batch name from vendor matches the modality, and build one command per load.
- Skip a fastq sample when its library prep has no command.
- Skip a stage when it is complete or already in progress.
- Submit commands through the `ocs` CLI within the configured job limit.
- Run a LIMS audit using the batch name from vendor after batch alignment submission attempts, unless `--audit false` is set.
- Write a JSON manifest with planned commands and submission results.
- Send submission summaries through AWS SES.

## Workflow

The OCS submission workflow loads fastq samples from one input source, builds and submits or dry-runs alignment and post-alignment commands, and writes a JSON manifest.

The export file from OCS Tracker provides metadata and stage statuses. For other input sources, the workflow looks up both on OCS. Load name inputs create one command and manifest row per load.

Summary emails are sent when there is something to report and `--email` is provided. Batch Processing runs audit by default after alignment submission attempts when `--email` is provided. The audit queries LIMS and sends reports for each batch name from vendor. Set `--audit true` to request audit for other batch runs, or `--audit false` to always skip it. Backlog runs never run audit. Dry runs print commands and write the manifest without submitting jobs, sending emails, or running LIMS audits.

```
Input (the export file from OCS Tracker / batch name from vendor / load names / fastq sample names)
        │
        ▼
┌─────────────────────────┐
│  Load Sample Metadata   │  read the export file from OCS Tracker or query OCS
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│  Check Stage Status     │  use statuses from the export file or check them on OCS
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
│  Run Audit (batch only)  │  audit alignment submissions by default, unless disabled
└─────────────────────────┘
```

## Inputs

### Parameter boundaries

The CLI groups parameters into three sections:

1. **Batch Processing:** `--batch-name-from-vendor`, `--batch-processing` (Use fastq sample names), `--force-submission`, and `--audit`.
2. **Backlog or Resequencing Runs:** `--ocs-tracker-exporter`, `--fastq-names`, and `--load-names`.
3. **Common Parameters:** `--modality` is required with every input source. `--email` and `--dry-run` work with either mode. `--config` selects the command configuration for either mode.

Use inputs from either Batch Processing or Backlog or Resequencing Runs. Supplying inputs from both sections stops the run with an error before metadata is loaded or jobs are submitted.

Providing batch name from vendor or force submission, or setting `--audit` or `--batch-processing` to `true`, selects Batch Processing. Both modality and batch name from vendor are required. If either is missing, the run stops with an error. Setting `--audit` or `--batch-processing` to `false` does not select Batch Processing. Backlog runs can use `--audit false`, but `--audit true` is rejected.

Backlog or Resequencing Runs is only used when there are no Batch Processing inputs. Provide `--modality RTX`, `--modality MTX`, or `--modality RFX` to select the workflow. Use `--modality MTX` for multiome loads containing both ATX and MTX fastq samples. Modality is not inferred from batch names from vendor. Submit each modality separately.

For backlog runs, the export file from OCS Tracker takes precedence over load names and fastq sample names. If no export file is provided, load names take precedence over fastq sample names. Only one input source is used during execution.

Use one of the following input sources:

### The export file from OCS Tracker

The export file from OCS Tracker is checked for the following headers:

- `Fastq Name`
- `Study Set`
- `Load Name`
- `Library Prep Method`
- `Organism` or `Organism Common Name`
- `Ingest`
- `Alignment`
- `Post Alignment`

Capitalization differences and spaces, underscores, or hyphens are accepted, as are clear minor typos. Ambiguous or
missing headers produce an error that identifies the expected field. `Batch Name From Vendor` is optional. When it is
absent, the capsule looks it up from OCS using each fastq sample name.

```bash
ocs-submission \
  --ocs-tracker-exporter /path/to/ocs_tracker_export.csv \
  --modality MTX \
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

For load names, the capsule retrieves every fastq sample associated with each load, checks status using the fastq sample whose batch name from vendor matches the selected modality, and creates one command and manifest row per load. For multiome loads, the row combines the GEX and ATAC fastq sample names and library prep names while using the MTX/GEX fastq sample for status and command configuration.

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
| `--modality` | Yes | Workflow modality: `RTX`, `MTX`, or `RFX`. Required with every input source |
| `--ocs-tracker-exporter` | No | Path to the export file from OCS Tracker |
| `--batch-name-from-vendor` | Batch Processing | Batch name from vendor. Cannot be combined with backlog inputs |
| `--load-names` | No | One or more sequencing load names. Creates one command per load |
| `--fastq-names` | No | One or more fastq sample names |
| `--force-submission` | No | Force `alignment` or `post-alignment` regardless of its current status. Alignment still requires completed ingest and post-alignment still requires completed alignment |
| `--email`, `-e` | No | Email for OCS job notifications and run summary emails. Required to generate and send audit reports |
| `--dry-run` | No | `true` or `false` (default `false`). Log commands without submitting jobs or sending summary or audit emails |
| `--audit` | No | Defaults to auditing batch alignment submission attempts. `true` requests audit for any batch run. `false` always disables audit. Requires `--email`. Backlog and dry runs never run audit |
| `--batch-processing` | No | Batch Processing only: `true` or `false` (default `false`). Use fastq sample names for RTX/RFX alignment and post-alignment commands |
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
| `<batch>_<modality>_missing_data.csv` | `/results` or current directory | Missing LIMS data report when a batch audit runs |
| `<batch>_lims_pull.csv` | `/results` or current directory | Full LIMS pull for the batch when a batch audit runs |

## Environment

| Variable | Used by | Purpose |
|---|---|---|
| `DATABASE_USERNAME` | LIMS audit | LIMS database user. Required when a batch audit runs |
| `DATABASE_PASSWORD` | LIMS audit | LIMS database password. Required when a batch audit runs |

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
├── inputs/                   # Load fastq sample metadata and stage statuses
├── integrations/             # Run OCS commands, read credentials, and send emails
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
