# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.5] - 2026-09-23

### Added

- Added `--load-names` input for submitting one alignment or post-alignment command per sequencing load.

### Changed

- OCS Tracker CSV headers now accept capitalization differences, spaces, underscores, hyphens, and clear minor typos.
  `Organism Common Name` is accepted as an alias for `Organism`.
- Multiome load manifests now combine the GEX and ATAC FASTQ names and library prep names into one row. The MTX/GEX
  record supplies the vendor batch, command configuration, and OCS status for that row.
- GEMX multiome alignment now uses Cell Ranger ARC 2.2.0 with `--create-bam=true`.
- `10xV4_PX` now uses Cell Ranger Multi 9.0.1 with BAM creation and is no longer sent through standard RNA-seq QC.
- Mouse CellFlex alignment now uses the GRCm39 FX v2 reference and probe set, Cell Ranger Multi 10.1.0, 128 vCPUs,
  and CellFlex QC 1.1.2 for the configured v1 and v2 library preps.
- Added reference aliases for coyote and organism names that may use hyphens or underscores. Common marmoset now uses
  `marmoset_ncbi_caljac240-pri_genome_arc-v2-2`.
- Renamed the internal `core` package to `workflow` to match its stage-related contents.

### Fixed

- Load-name submissions now check OCS status only for the FASTQ whose vendor batch matches the requested modality.
- Load-name submissions now create one command per load instead of one command for each FASTQ in the load.
- OCS metadata lookup now uses the singular `--fastq-name` flag. RTX and RFX batch submissions continue to use the
  plural `--fastq-names` flag required by alignment commands.
- Dry-run, submission, and job-limit logs now show the load name when the command uses `--load-names`.
- Corrected the malformed `10xV3.1_FX` probe-set entry and routed CellFlex post-alignment to `tenx_cellflex_qc`.

## [0.1.4] - 2026-08-22

### Changed

- Organized the submission package into configuration, core, commands, inputs, and integrations subpackages.
- CI now blocks on formatting, linting, mypy, tests, package builds, and CLI entry-point checks.
- CI now reports test coverage.

### Removed

- Removed the obsolete running-jobs database integration and its status fallback.
- Removed the unused tag-triggered GitHub Release workflow and its release validator.

## [0.1.3] - 2026-08-11

### Added

- `--batch-processing true|false` option. When enabled for RTX or RFX, alignment and post-alignment commands use
  `--fastq-names`; otherwise they use `--load-names`.
- RFX CellFlex LIMS audit using the same audit rules as RTX.

### Changed

- Moved the release-version validator to `scripts/release/check_version.py`.
- Replaced the legacy MTX and RTX LIMS audit queries with CTE-based RNA/multiome and CellFlex metadata queries.

### Fixed

- Samples whose library prep is not configured for a scheduled stage are now skipped instead of stopping command
  construction, and their fastq names are reported in the logs and submission summary email.
- Commands now support a single probe set shared by an organism as well as probe sets mapped by library prep.

## [0.1.2] - 2026-07-14

### Added

- Optional library-prep-specific reference selection within an organism and modality.

### Changed

- Release workflow now rejects tags that do not point at commits on `main`.

### Fixed

- Tracker DB pool now discards idle connections the server has already closed
  (e.g. after job-limit wait sleeps), avoiding false OCS submission failures.
- Post-alignment config lookup is skipped when only alignment is scheduled,
  including forced alignment submissions.

### Removed

- Release-version badge from the README.

## [0.1.1] - 2026-07-06

### Added

- Per-library-prep post-alignment command configs in `config.jsonc`
- Config-loop tests and manifest schema checks
- Tag-driven release workflow that verifies the tag against `pyproject.toml`
  and `CHANGELOG.md`, runs the tests, and publishes a GitHub release with notes
  from the changelog

### Changed

- README section headings and workflow overview wording
- Ruff line length set to 120

### Fixed

- Audit CSV attachments in SES raw email
- Notification sender address for summary emails
- Fastq stage status log labels

## [0.1.0] - 2026-06-26

### Added

- OCS submission capsule: ingest → align → postalign workflow
- `config.jsonc` templates for MTX, RTX, and RFX
- Job manifest output and PostgreSQL tracker integration
- LIMS audit and AWS SES email summaries

[Unreleased]: https://github.com/AllenInstitute/OCS-Submission-Capsule-API/compare/v0.1.5...HEAD
[0.1.5]: https://github.com/AllenInstitute/OCS-Submission-Capsule-API/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/AllenInstitute/OCS-Submission-Capsule-API/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/AllenInstitute/OCS-Submission-Capsule-API/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/AllenInstitute/OCS-Submission-Capsule-API/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/AllenInstitute/OCS-Submission-Capsule-API/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/AllenInstitute/OCS-Submission-Capsule-API/releases/tag/v0.1.0
