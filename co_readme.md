# OCS Submission Capsule

This Code Ocean capsule checks FASTQ ingest, alignment, and post-alignment status on OCS, builds alignment and post-alignment commands from configuration, optionally submits those jobs with the OCS CLI, tracks submitted jobs in PostgreSQL, can run a LIMS audit, and sends summary emails through a Code Ocean email capsule.

## Quick Start

1. Run a dry run first:

```bash
ocs-submission \
  --modality MTX \
  --batch-name-from-vendor MTX-22068 \
  --dry-run true
```

2. If the planned commands look correct, rerun without `--dry-run true`.

You can also run it as a module:

```bash
python -m ocs_submission --modality MTX --batch-name-from-vendor MTX-22068 --dry-run true
```

## What This Capsule Does

- Checks ingest, alignment, and post-alignment status for each FASTQ.
- Loads FASTQ metadata from OCS Tracker exporter CSVs, vendor batch names, or explicit FASTQ names.
- Builds alignment and post-alignment OCS commands from `config.jsonc`.
- Skips work that is already complete or currently in progress.
- Tracks submitted jobs in PostgreSQL so in-flight jobs can be checked later.
- Optionally runs a LIMS audit for each unique vendor batch.
- Writes a JSON manifest of planned and attempted commands.
- Sends summary emails for successful and failed submissions.

## Input Modes

Exactly one of these is required:

### Mode 1: OCS Tracker Exporter

```bash
ocs-submission \
  --ocs-tracker-exporter /path/to/ocs_tracker_export.csv \
  --modality RTX \
  --dry-run true
```

### Mode 2: Vendor Batch

```bash
ocs-submission \
  --batch-name-from-vendor MTX-22068 \
  --modality MTX \
  --dry-run true
```

### Mode 3: Explicit FASTQ Names

```bash
ocs-submission \
  --fastq-names NY-MX22068-2 NY-MX22068-3 \
  --modality MTX \
  --dry-run true
```

## Command-Line Options

| Option | Required | Description |
|---|---|---|
| `--modality` | Yes | Workflow modality: `RTX`, `MTX`, or `RFX` |
| `--ocs-tracker-exporter` | No | Path to an OCS Tracker export CSV |
| `--batch-name-from-vendor` | No | Vendor batch name |
| `--fastq-names` | No | One or more FASTQ names |
| `--force-submission` | No | Force submission of `alignment` or `post-alignment` |
| `--email`, `-e` | No | Override the notification email |
| `--dry-run` | No | `true` or `false`; default is `false` |
| `--audit` | No | `true` or `false`; default is `false` |
| `--config` | No | Path to a JSONC config file; defaults to bundled `ocs_submission/config.jsonc` |

Notes:
- `--batch-name-from-vendor` and `--fastq-names` are mutually exclusive.
- One of `--ocs-tracker-exporter`, `--batch-name-from-vendor`, or `--fastq-names` must be provided.

## How Status Checking Works

The capsule checks status in two places:

### Step 1: Query OCS

It queries OCS for the latest:
- ingest result
- alignment result
- post-alignment result

When all FASTQs belong to one vendor batch, it can fetch the latest results batch-wide and match them back to each FASTQ.

### Step 2: Fall Back to the Tracker Database

If OCS has no latest alignment or post-alignment result yet, the capsule checks the `running_jobs` table to see whether this capsule already submitted the job and whether it is still in progress.

## Status-Based Submission Logic

For each FASTQ:

1. If ingest is not complete, alignment is skipped.
2. If alignment is complete, alignment is skipped.
3. If alignment is in progress, alignment is skipped.
4. Otherwise, alignment is prepared for submission.
5. Post-alignment is only prepared when alignment is already complete and alignment is not being submitted in the same pass.
6. If post-alignment is complete or in progress, it is skipped.
7. Otherwise, post-alignment is prepared for submission.

`--force-submission alignment` or `--force-submission post-alignment` overrides the normal decision for that stage.

## Job Tracking Database

The capsule uses a PostgreSQL `running_jobs` table to track submissions.

Stored fields include:
- `fastq_name`
- `job_type`
- `command`
- `demand_id`
- `status`
- `batch_name_from_vendor`
- `created_at`
- `updated_at`

The tracker DB is used to:
- record newly submitted jobs
- look up tracked jobs by FASTQ and stage
- refresh tracked job status from OCS
- remove jobs when they are no longer needed

## Configuration

The capsule reads configuration from:

```text
ocs_submission/config.jsonc
```

Key sections include:
- `references`: maps organisms and modalities to reference names
- `probe_sets_by_organism`: optional probe-set mapping for supported organism/library-prep combinations
- `chemistry_by_library_prep`: maps library prep names to chemistry strings
- `workflows`: alignment and post-alignment command templates for `MTX`, `RTX`, and `RFX`
- `job_settings`: submission limits
- `status_mappings`: defines which statuses count as complete

Command templates support placeholders such as:
- `{reference_name}`
- `{load_name}`
- `{email}`
- `{chemistry}`
- `{probe_set}`
- `{execution_vcpus}`

## Outputs

### Data Manifest

The capsule writes:

```text
ocs_job_commands_manifest.json
```

Location:
- `/results` when that directory exists
- otherwise the current working directory

### Audit Files

When `--audit true` is enabled, audit CSVs are also written to `/results` or the current working directory.

## Email Notifications

The capsule sends summary emails through a Code Ocean email capsule.

Summary emails include:
- successful submissions
- failed submissions
- FASTQ name
- load name
- job type
- submission time
- command
- demand ID for successful submissions
- error details for failed submissions

When `--audit true` is enabled, the audit output can also be emailed.

## Force Submission Examples

```bash
ocs-submission \
  --batch-name-from-vendor MTX-22068 \
  --modality MTX \
  --force-submission alignment
```

```bash
ocs-submission \
  --batch-name-from-vendor MTX-22068 \
  --modality MTX \
  --force-submission post-alignment
```

## Job Limits and Throttling

- The capsule checks the current number of in-progress OCS alignment and post-alignment jobs before submitting more.
- The maximum allowed running-job count comes from `config.jsonc`.
- Each workflow template can also define a `spacing` delay between submissions.

## Main Files

- `ocs_submission/main.py`: CLI entrypoint
- `ocs_submission/__main__.py`: supports `python -m ocs_submission`
- `ocs_submission/fastq_info_fetcher.py`: loads metadata and stage statuses
- `ocs_submission/ocs_command_builder.py`: builds alignment and post-alignment command records
- `ocs_submission/ocs_cli.py`: wraps the `ocs` CLI, checks running-job limits, and submits jobs
- `ocs_submission/running_jobs_db.py`: PostgreSQL tracker helpers
- `ocs_submission/emails.py`: summary and audit emails
- `ocs_submission/config.jsonc`: workflow and status configuration
- `ocs_submission/audit/`: LIMS audit logic and SQL

## Dependencies

- Python 3.10+
- `ocs` CLI available on `PATH`
- Python packages from `pyproject.toml`
- PostgreSQL access for the tracker database
- Code Ocean credentials for the email capsule

## Environment Variables

The code currently uses these environment variables:

- `CO_DOMAIN`
- `ACCESS_TOKEN`
- `EMAIL_CAPSULE_ID`
- `CO_COMPUTATION_ID`
- `DATABASE_USERNAME`
- `DATABASE_PASSWORD`

## Important: Code Ocean Environment Behavior

Post-install runs during the environment build phase. When that phase finishes, Code Ocean bakes the filesystem changes into the image:
- the cloned repo
- any built virtual environment
- any binaries or symlinks created during post-install

However, the shell environment from that post-install session is discarded:
- environment variables exported during post-install are not automatically present in later shells
- this affects both capsule runs and interactive terminal sessions

If the capsule depends on environment variables, make sure they are set again in the runtime environment rather than assuming they persist from post-install.
