# OCS Submission

Loads FASTQ status from OCS, builds alignment and post-alignment commands from configuration templates, submits jobs to OCS (with optional dry run), optionally runs a LIMS audit per batch, writes a JSON data manifest, and emails a run summary via the Code Ocean email capsule.

## Installation

```bash
pip install -e .
```

Requires Python 3.10+ and the `ocs` CLI on `PATH`.

## Environment

The package reads the following environment variables:

| Variable              | Used by           | Purpose                                   |
|-----------------------|-------------------|-------------------------------------------|
| `CO_DOMAIN`           | `emails`          | Code Ocean domain for the email capsule   |
| `ACCESS_TOKEN`        | `emails`          | Code Ocean access token                   |
| `EMAIL_CAPSULE_ID`    | `emails`          | Capsule id to run for summary emails      |
| `DATABASE_USERNAME`   | `audit`           | LIMS database user                        |
| `DATABASE_PASSWORD`   | `audit`           | LIMS database password                    |
| `RUNNING_JOBS_DB_*`   | `running_jobs_db` | Connection settings for the tracker DB    |

## Usage

After installation the console script `ocs-submission` is available:

```bash
ocs-submission \
  --modality RTX \
  --batch-name-from-vendor RTX-34056 \
  --dry-run true \
  --force-submission alignment \
  --audit true
```

You can also invoke it as a module (from the repo root, with the package on `PYTHONPATH`):

```bash
python -m ocs_submission --modality MTX --batch-name-from-vendor MTX-22048 --dry-run true
```

### CLI flags

| Flag                        | Required | Description                                                                  |
|-----------------------------|----------|------------------------------------------------------------------------------|
| `--modality`                | yes      | `RTX`, `MTX`, or `RFX`                                                       |
| `--ocs-tracker-exporter`    |          | Path to an OCS Tracker export CSV                                            |
| `--batch-name-from-vendor`  |          | Vendor batch name (mutually exclusive with `--fastq-names`)                  |
| `--fastq-names`             |          | Explicit FASTQ names (mutually exclusive with `--batch-name-from-vendor`)    |
| `--force-submission`        |          | `alignment` or `post-alignment`                                              |
| `--email` / `-e`            |          | Override notification email                                                  |
| `--dry-run`                 |          | `true` / `false` (default `false`) — log commands without executing          |
| `--audit`                   |          | `true` / `false` (default `false`) — run LIMS audit per unique vendor batch  |
| `--config`                  |          | Path to JSONC config (default: bundled `ocs_submission/config.jsonc`)        |

Exactly one of `--ocs-tracker-exporter`, `--batch-name-from-vendor`, or `--fastq-names` is required.

## Outputs

- **Data manifest**: `ocs_job_commands_manifest.json` under `/results` when that directory exists, otherwise the current working directory (one row per FASTQ with planned commands and execution fields).
- **Audit CSVs** (when `--audit true`): written next to the manifest (`/results` or `.`) as `<batch>_<modality>_missing_data.csv` and `<batch>_lims_pull.csv`.

## Package layout

```
ocs_submission/
├── __init__.py
├── __main__.py              # `python -m ocs_submission` entry
├── main.py                  # CLI entry (exposed as `ocs-submission`)
├── config.jsonc             # Workflow templates and status mappings
├── ocs_cli.py               # `ocs` CLI wrapper, job limits, command submission
├── ocs_command_builder.py   # Build alignment + post-alignment commands
├── fastq_info_fetcher.py    # Load FASTQ records from exporter / batch / names
├── emails.py                # Summary + audit email via Code Ocean email capsule
├── running_jobs_db.py       # Tracker PostgreSQL helpers
└── audit/
    ├── __init__.py
    ├── audit.py             # LIMS audit (exports `run_audit`)
    ├── lims_mtx_ocs.sql
    └── lims_rtx_ocs.sql
```
