import sys
from unittest.mock import create_autospec

import pandas as pd
import pytest

from ocs_submission import main

EMAIL = "test@example.org"


@pytest.fixture
def workflow(monkeypatch, tmp_path, config, fastq_records):
    records_by_source = {
        "batch": fastq_records.iloc[:1],
        "exporter": fastq_records,
        "fastq_names": fastq_records[fastq_records["library_prep_method_name"] == "10xMultX_GEX"],
        "load_names": fastq_records[fastq_records["load_name"] == "3796_A01"],
    }
    loaders = {}
    for source, records in records_by_source.items():
        name = f"load_fastq_records_df_from_{source}"
        loader = create_autospec(getattr(main, name), return_value=records.copy())
        monkeypatch.setattr(main, name, loader)
        loaders[source] = loader
    config["job_settings"] = {"limit": 5}
    monkeypatch.setattr(main, "load_jsonc_config", lambda path: config)
    monkeypatch.setattr(
        main,
        "execute_ocs_submission_commands",
        create_autospec(
            main.execute_ocs_submission_commands,
            side_effect=lambda ocs_job_commands_df, **kwargs: ocs_job_commands_df,
        ),
    )
    for name in ("send_audit_email", "send_command_summary_email"):
        monkeypatch.setattr(main, name, create_autospec(getattr(main, name)))
    manifest = tmp_path / "manifest.json"
    monkeypatch.setattr(main, "DATA_MANIFEST_PATH", str(manifest))
    return loaders, manifest


@pytest.mark.parametrize(
    "audit_arguments",
    [
        pytest.param([], id="default_audit"),
        pytest.param(["--audit", "true"], id="audit_enabled"),
        pytest.param(["--audit", "false"], id="audit_disabled"),
    ],
)
def test__main__batch_dry_run_uses_common_parameters(monkeypatch, workflow, audit_arguments):
    loaders, manifest = workflow
    monkeypatch.setattr(
        sys,
        "argv",
        (
            "ocs-submission --modality MTX --batch-name-from-vendor MTX-32013 "
            "--force-submission alignment --batch-processing true "
            f"--email {EMAIL} --dry-run true"
        ).split()
        + audit_arguments,
    )

    main.main()

    loaders["batch"].assert_called_once_with("MTX-32013")
    for source in ("exporter", "fastq_names", "load_names"):
        loaders[source].assert_not_called()
    rows = pd.read_json(manifest)
    assert len(rows) == 1
    assert rows["force_submission"].tolist() == ["alignment"]
    assert rows["notify_email"].tolist() == [EMAIL]
    assert rows["dry_run"].all()
    main.send_audit_email.assert_not_called()
    main.send_command_summary_email.assert_not_called()


@pytest.mark.parametrize(
    "arguments",
    [
        pytest.param("--batch-name-from-vendor MTX-32013 --fastq-names NW-MX32013-2", id="batch_name_from_vendor"),
        pytest.param("--force-submission alignment --fastq-names NW-MX32013-2", id="force_submission"),
        pytest.param("--batch-processing true --fastq-names NW-MX32013-2", id="use_fastq_names"),
        pytest.param("--audit true --fastq-names NW-MX32013-2", id="audit"),
        pytest.param(
            "--batch-name-from-vendor MTX-32013 --ocs-tracker-exporter tracker.csv",
            id="batch_and_tracker_export",
        ),
        pytest.param(
            "--batch-name-from-vendor MTX-32013 --load-names 3796_A01",
            id="batch_and_load_names",
        ),
    ],
)
def test__main__rejects_mixed_batch_and_backlog_inputs(monkeypatch, capsys, workflow, arguments):
    loaders, manifest = workflow
    monkeypatch.setattr(sys, "argv", ["ocs-submission", "--modality", "MTX", *arguments.split()])

    with pytest.raises(SystemExit, match="2"):
        main.main()

    assert (
        "Use inputs from either Batch Processing or Backlog or Resequencing Runs, not both" in capsys.readouterr().err
    )
    for loader in loaders.values():
        loader.assert_not_called()
    main.execute_ocs_submission_commands.assert_not_called()
    main.send_audit_email.assert_not_called()
    assert not manifest.exists()


@pytest.mark.parametrize(
    "arguments",
    [
        pytest.param(["--batch-name-from-vendor", "MTX-32013"], id="batch_name_from_vendor"),
        pytest.param(["--fastq-names", "NW-MX32013-2"], id="fastq_samples"),
        pytest.param(["--load-names", "3796_A01"], id="load_names"),
        pytest.param(["--ocs-tracker-exporter", "tracker.csv"], id="ocs_tracker_export"),
    ],
)
def test__main__requires_modality_for_each_input_source(monkeypatch, capsys, workflow, arguments):
    loaders, manifest = workflow
    monkeypatch.setattr(sys, "argv", ["ocs-submission", *arguments])

    with pytest.raises(SystemExit, match="2"):
        main.main()

    assert "the following arguments are required: --modality" in capsys.readouterr().err
    for loader in loaders.values():
        loader.assert_not_called()
    main.execute_ocs_submission_commands.assert_not_called()
    assert not manifest.exists()


@pytest.mark.parametrize(
    "batch_arguments",
    [
        pytest.param(["--force-submission", "alignment"], id="force_submission"),
        pytest.param(["--batch-processing", "true"], id="use_fastq_names"),
        pytest.param(["--audit", "true"], id="audit"),
    ],
)
def test__parse_args__batch_requires_batch_name(monkeypatch, capsys, batch_arguments):
    monkeypatch.setattr(sys, "argv", ["ocs-submission", "--modality", "MTX", *batch_arguments])

    with pytest.raises(SystemExit, match="2"):
        main.parse_args()

    assert "Batch Processing requires --batch-name-from-vendor" in capsys.readouterr().err


@pytest.mark.parametrize(
    "common_arguments, dry_run",
    [
        pytest.param(["--dry-run", "true"], True, id="dry_run"),
        pytest.param(["--dry-run", "false"], False, id="submit"),
        pytest.param(["--audit", "false", "--batch-processing", "false"], False, id="batch_options_disabled"),
    ],
)
@pytest.mark.parametrize(
    "arguments, source, loader_arguments, row_count",
    [
        pytest.param(["--ocs-tracker-exporter", "tracker.csv"], "exporter", "tracker.csv", 3, id="tracker"),
        pytest.param(
            ["--fastq-names", "  NW-MX32013-2, NW-MX32021-10  "],
            "fastq_names",
            ["NW-MX32013-2", "NW-MX32021-10"],
            2,
            id="fastq",
        ),
        pytest.param(["--load-names", "3796_A01"], "load_names", ["3796_A01"], 1, id="load"),
        pytest.param(
            ["--ocs-tracker-exporter", "tracker.csv", "--load-names", "ignored", "--fastq-names", "ignored"],
            "exporter",
            "tracker.csv",
            3,
            id="tracker_ignores_other_sources",
        ),
        pytest.param(
            ["--load-names", "3796_A01", "--fastq-names", "ignored"],
            "load_names",
            ["3796_A01"],
            1,
            id="load_ignores_fastq_source",
        ),
    ],
)
def test__main__backlog_uses_common_parameters(
    monkeypatch, workflow, arguments, source, loader_arguments, row_count, common_arguments, dry_run
):
    loaders, manifest = workflow
    monkeypatch.setattr(
        sys,
        "argv",
        ["ocs-submission", *arguments, "--modality", "MTX", "--email", EMAIL, *common_arguments],
    )

    main.main()

    if source == "load_names":
        loaders[source].assert_called_once_with(loader_arguments, modality="MTX")
    else:
        loaders[source].assert_called_once_with(loader_arguments)
    for other_source, loader in loaders.items():
        if other_source != source:
            loader.assert_not_called()
    rows = pd.read_json(manifest)
    assert len(rows) == row_count
    assert rows["modality"].tolist() == ["MTX"] * row_count
    assert rows["notify_email"].tolist() == [EMAIL] * row_count
    assert rows["dry_run"].tolist() == [dry_run] * row_count
    assert rows["force_submission"].isna().all()
    main.send_audit_email.assert_not_called()
    if dry_run:
        main.send_command_summary_email.assert_not_called()
    else:
        assert main.send_command_summary_email.call_args.kwargs["notify_email"] == EMAIL


@pytest.mark.parametrize(
    "audit_arguments, align_status, postalign_status, audit_expected",
    [
        pytest.param([], "NOT COMPLETED", "NOT COMPLETED", True, id="alignment_default_audit"),
        pytest.param(["--audit", "true"], "NOT COMPLETED", "NOT COMPLETED", True, id="alignment_audit_enabled"),
        pytest.param(["--audit", "false"], "NOT COMPLETED", "NOT COMPLETED", False, id="alignment_audit_disabled"),
        pytest.param([], "COMPLETED", "NOT COMPLETED", False, id="post_alignment_default_no_audit"),
        pytest.param(["--audit", "true"], "COMPLETED", "NOT COMPLETED", True, id="post_alignment_audit_enabled"),
        pytest.param(["--audit", "false"], "COMPLETED", "NOT COMPLETED", False, id="post_alignment_audit_disabled"),
        pytest.param([], "COMPLETED", "COMPLETED", False, id="no_submission_default_no_audit"),
        pytest.param(["--audit", "true"], "COMPLETED", "COMPLETED", True, id="audit_requested_without_submission"),
        pytest.param(["--audit", "false"], "COMPLETED", "COMPLETED", False, id="no_submission_audit_disabled"),
    ],
)
def test__main__batch_audit_follows_submission_and_flag(
    monkeypatch, workflow, audit_arguments, align_status, postalign_status, audit_expected
):
    loaders, manifest = workflow
    loaders["batch"].return_value.loc[:, "align_status"] = align_status
    loaders["batch"].return_value.loc[:, "postalign_status"] = postalign_status
    monkeypatch.setattr(
        sys,
        "argv",
        f"ocs-submission --modality MTX --batch-name-from-vendor MTX-32013 --email {EMAIL}".split() + audit_arguments,
    )

    main.main()

    rows = pd.read_json(manifest)
    assert rows["align_should_execute"].tolist() == [align_status == "NOT COMPLETED"]
    assert rows["postalign_should_execute"].tolist() == [
        align_status == "COMPLETED" and postalign_status == "NOT COMPLETED"
    ]
    if audit_expected:
        main.send_audit_email.assert_called_once_with("MTX-32013", EMAIL)
    else:
        main.send_audit_email.assert_not_called()
    main.send_command_summary_email.assert_called_once()


def test__main__explicit_modality_does_not_require_a_recognized_batch_name(monkeypatch, workflow):
    loaders, manifest = workflow
    loaders["fastq_names"].return_value["batch_name_from_vendor"] = "UNKNOWN-1"
    monkeypatch.setattr(
        sys,
        "argv",
        ["ocs-submission", "--fastq-names", "NW-MX32013-2", "NW-MX32021-10", "--modality", "MTX", "--dry-run", "true"],
    )

    main.main()

    rows = pd.read_json(manifest)
    assert rows["modality"].tolist() == ["MTX", "MTX"]
    assert rows["align_should_execute"].all()
    main.send_audit_email.assert_not_called()


def test__parse_args__help_lists_modality_in_common_parameters(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["ocs-submission", "--help"])

    with pytest.raises(SystemExit, match="0"):
        main.parse_args()

    help_text = capsys.readouterr().out
    batch_help = help_text.split("Batch Processing:")[1].split("Backlog or Resequencing Runs:")[0]
    common_help = help_text.split("Common Parameters:")[1]
    assert "--modality" not in batch_help
    assert "--modality" in common_help
