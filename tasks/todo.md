# DATS/PTS migration checklist

- [x] Complete the live DATS/PTS contract for `NW-FX38025-7`.
- [x] Verify LIMS2 supplies the pre-alignment organism and sample metadata for `NW-FX38025-7`.
- [x] Adopt the `cell-omics-data-export` `DB_CONNECTION_STRING` boundary and validate the production URI form.
- [ ] Define LIMS2/DATS/PTS field ownership and conflict handling.
- [x] Implement the typed PTS/DATS read adapter.
- [x] Implement the parameterized LIMS2 metadata adapter.
- [ ] Add contract tests for stage lineage and process state.
- [ ] Add LIMS2 metadata and connection tests.
- [x] Migrate capsule metadata/status reads.
- [x] Delete the capsule `running_jobs_db` module, schema, configuration, initialization, reads, writes, and tests.
- [ ] Add the DATS/PTS demand-correlation check needed to replace the old submission tracker behavior.
- [x] Remove all OCS CLI metadata and status reads. Keep OCS CLI only for submission.
- [x] Define explicit errors and pending states for missing or delayed source records. Do not add fallbacks.
- [x] Verify the CLI is used only for submission.
- [x] Run focused tests, lint, typing, and the full relevant test suite.

## Checkpoint

- [x] Remove OCS metadata reads after verifying LIMS2 and DATS/PTS coverage.
- [ ] Prove DATS/PTS demand correlation before treating a previously submitted job as already running.
- [x] Do not add fallback behavior for missing LIMS, PTS, or DATS data. Return the contract error or pending state.
