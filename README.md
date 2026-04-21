# OCS Submission

Loads FASTQ status from OCS, builds alignment and post-alignment commands from configuration templates, submits jobs to OCS (with optional dry run), optionally runs a LIMS audit per alignment, writes a JSON data manifest, and emails a run summary via the Code Ocean email capsule.

## Installation

```bash
pip install -e .
```

Requires Python 3.10+ and the `ocs` CLI on `PATH`.

## Environment

The package reads the following environment variables:

| Variable            | Used by               | Purpose                                   |
|---------------------|-----------------------|-------------------------------------------|
| `CO_DOMAIN`         | `notifications`       | Code Ocean domain for the email capsule   |
| `ACCESS_TOKEN`      | `notifications`       | Code Ocean access token                   |
| `EMAIL_CAPSULE_ID`  | `notifications`       | Capsule id to run for summary emails      |
| `DATABASE_USERNAME` | `audit`               | LIMS database user                        |
| `DATABASE_PASSWORD` | `audit`               | LIMS database password                    |
| `RUNNING_JOBS_DB_*` | `running_jobs_db`     | Connection settings for the tracker DB    |

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
| `--audit`                   |          | `true` / `false` (default `false`) — run LIMS audit for each alignment exec  |
| `--config`                  |          | Path to JSONC config (default: bundled `ocs_submission/config.jsonc`)        |

Exactly one of `--ocs-tracker-exporter`, `--batch-name-from-vendor`, or `--fastq-names` is required.

## Outputs

- **Data manifest**: `./.data_manifest.json` (one row per FASTQ with `alignment_*` and `post_alignment_*` columns).
- **Audit CSVs** (when `--audit true`): `ocs_submission/audit/out/<batch_name>/`.

## Package layout

```
ocs_submission/
├── __init__.py
├── main.py                    # CLI entry (exposed as `ocs-submission`)
├── config.jsonc               # Workflow templates and status mappings
├── ocs_cli.py                 # Thin wrapper over the `ocs` CLI
├── ocs_command_builder.py     # Build alignment + post-alignment commands
├── execution.py               # Submit commands, handle limits / spacing / audit
├── fetch_fastq_ocs_records.py # Load FASTQ records from exporter / batch / names
├── notifications.py           # Summary email via Code Ocean email capsule
├── running_jobs_db.py         # Tracker PostgreSQL helpers
└── audit/
    ├── __init__.py
    ├── audit.py               # LIMS audit (exports `run_audit`)
    ├── lims_mtx_ocs.sql
    └── lims_rtx_ocs.sql
```
