# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Post-alignment config lookup is skipped when only alignment is scheduled,
  including forced alignment submissions.

### Changed

- Release badge now points at the latest published GitHub Release instead of
  the latest raw Git tag.
- Release workflow now rejects tags that do not point at commits on `main`.

## [0.1.1] - 2026-07-06

### Added

- Per-library-prep post-alignment command configs in `config.jsonc`
- Config-loop tests and manifest schema checks
- Tag-driven release workflow that verifies the tag against `pyproject.toml`
  and `CHANGELOG.md`, runs the tests, and publishes a GitHub release with notes
  from the changelog (`scripts/check_release_version.py`)

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

[Unreleased]: https://github.com/AllenInstitute/OCS-Submission-Capsule-API/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/AllenInstitute/OCS-Submission-Capsule-API/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/AllenInstitute/OCS-Submission-Capsule-API/releases/tag/v0.1.0
