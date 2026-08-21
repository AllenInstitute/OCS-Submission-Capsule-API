# Implementation Plan: DATS/PTS-backed OCS reads

## Overview

Move FASTQ metadata, stage status, process lineage, output asset, and storage lookups to the
typed DATS/PTS client package. Use LIMS2 for sample and library metadata that is not present in
pre-alignment PTS records. Retain the `ocs` CLI only for submitting alignment and post-alignment
demands. Delete the capsule `running_jobs_db` implementation and its configuration entirely.

## Architecture decisions

- PTS is the source for process identity, process type, state, timestamps, metadata, inputs, and outputs.
- DATS is the source for digital asset identity, asset instances, storage, and download URLs.
- LIMS2 is the source for sample and experiment metadata, including organism, species, study,
  load, library preparation, sample, and vendor batch fields.
- There are no fallback sources. A missing, delayed, or ambiguous value is an explicit error or
  pending state defined by the contract. The application must not silently consult another source,
  infer a value, or use a local cache.
- The LIMS connection must follow the proven `cell-omics-data-export` pattern: one
  `DB_CONNECTION_STRING` boundary, a typed PostgreSQL data fetcher, parameterized SQL, and no
  credential values in source files or logs. The exact `postgresql+pg8000` URI form must be
  validated against the chosen driver before implementation.
- Stage resolution follows process lineage and explicit process-type configuration. It must not choose an arbitrary first output or newest process without validating type, input lineage, and state.
- FASTQ metadata is composed by joining the LIMS record for the FASTQ name with the DATS/PTS
  record for the same FASTQ. LIMS is not used to infer process status or output locations.
- This capsule's submission manifest remains an application artifact. It is not a replacement for DATS/PTS status.

## Task list

### Phase 1: Contract and adapter

- [x] Confirm live DATS/PTS records for `NW-FX38025-7`, including metadata keys, process types, states, inputs, outputs, and DATS asset instances.
- [x] Confirm live LIMS2 metadata for `NW-FX38025-7` using the read-only tunnel. The query returned organism, species, study, sample, vendor batch, load, and library-prep fields.
- [x] Confirm the production LIMS connection boundary using `DB_CONNECTION_STRING` and the same driver/configuration pattern as `cell-omics-data-export`.
- [ ] Define the normalized FASTQ metadata contract and conflict rules for LIMS versus PTS fields.
- [ ] Define provenance on normalized fields so every value identifies LIMS2, PTS, or DATS as its source.
- [ ] Verify LIMS query cardinality and modality coverage for RNA-seq, multiome, and cell-flex inputs.
- [x] Add a DATS/PTS read adapter using the installed schema types, bounded API queries, deterministic stage selection, and explicit missing/ambiguous result errors.
- [x] Add a parameterized LIMS2 read adapter using the existing metadata SQL or a focused query template.
- [ ] Add contract fixtures and tests for ingest, alignment, post-QC, running, failed, retried, and missing-output cases.
- [ ] Add LIMS fixtures and tests for one row, no row, duplicate rows, and conflicting metadata.

### Checkpoint: read contract

- [x] The adapter can return one normalized FASTQ record without invoking `ocs`.
- [x] The adapter returns actual DATS download URLs or storage locations for every available stage.
- [x] Metadata coverage is documented and verified across LIMS2 and DATS/PTS.

### Phase 2: capsule migration

- [x] Replace OCS CLI metadata and status reads with the adapter.
- [x] Join LIMS2 metadata with DATS/PTS process and asset records by FASTQ name and validated lineage.
- [x] Delete `running_jobs_db.py`, remove `RUNNING_JOBS_DB_URL`, and remove every local job-tracking database call.
- [x] Ensure no replacement local status store, demand cache, or fallback tracker is introduced.
- [x] Remove all OCS CLI metadata and status reads. OCS CLI is submission-only.
- [x] Keep the OCS CLI only for job submission and demand ID extraction.

### Checkpoint: end-to-end

- [x] A FASTQ lookup works without OCS CLI read commands.
- [x] A submission still reaches OCS and records the returned demand ID in the manifest.
- [ ] A submitted OCS demand can be correlated to its PTS process before the next submission decision.
- [ ] Repeated submissions do not occur when PTS visibility is delayed, a process is retried, or a prior demand has no output yet.

## Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Organism/study metadata is not stored in DATS/PTS | High | Verify live records before removing OCS metadata reads |
| PTS has multiple retries for one stage | High | Select by process type, lineage, state, and explicit retry policy |
| One process has multiple outputs | High | Resolve output types and reject ambiguous records |
| DATS download URLs are temporary or storage-dependent | Medium | Preserve asset/storage identity and test URL freshness |
| Deleting `running_jobs_db` causes duplicate submissions | Critical | Implement and test demand-to-PTS correlation, delayed-visibility behavior, retry handling, and concurrent submission behavior before deletion |
| PTS process is not visible immediately after OCS submission | High | Define an explicit pending-submission correlation policy. Do not treat missing PTS data as safe to submit again |
| LIMS query returns multiple rows or misses a supported modality | High | Validate cardinality and modality-specific query templates. Reject ambiguous metadata instead of selecting an arbitrary row |
| Connection URI syntax differs from the installed PostgreSQL driver | High | Test the exact production `DB_CONNECTION_STRING` at the adapter boundary and use the driver-native URI form |

## Open questions

- Which PTS process types and state names are authoritative for ingest, alignment, and post-QC in the target environment?
- Which PTS fields, if any, correlate an OCS demand ID to a process before outputs are created?
- Which LIMS2 query template is authoritative for each supported modality, and what is the expected row cardinality?
- Should the final application use DATS download URLs, S3 URIs, or both as its public location field?

## Verified live sample: NW-FX38025-7

- PTS returned one `prod-ocs-ingest` process in `SUCCESS` state.
- The ingest process has seven output assets. The asset tagged `fastq_name: NW-FX38025-7` resolves through DATS to an S3 location.
- Ingest metadata includes `fastq_name`, `studies`, `sequencing_vendor`, and `batch_name_from_vendor`. For this sample, the study is `10x_SCH_slice_pilot` and the vendor batch is `RFX-38025`.
- No downstream `prod-ocs-align` process was found for the `NW-FX38025-7` ingest asset, so no post-QC process can be reached through lineage.
- The ingest process metadata does not include `organism_common_name` or an equivalent organism field. Organism appeared in alignment process metadata for other samples, but that cannot help when alignment has not started.
- LIMS2 supplies the pre-alignment organism and library metadata, so the metadata migration can use a LIMS2 + DATS/PTS composition.
- The explicit removal target is the local `running_jobs_db` module, schema, environment variable, initialization, writes, reads, and tests. Its replacement must be DATS/PTS process and demand correlation, not another local tracker.
