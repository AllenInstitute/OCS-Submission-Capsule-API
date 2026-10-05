import sys
from unittest.mock import create_autospec

import pandas as pd
import pytest

from ocs_submission import main

EMAIL = "test@example.org"


@pytest.fixture
def workflow(monkeypatch, tmp_path, config, make_fastq_record):
    records = pd.DataFrame([vars(make_fastq_record(fastq_name=name)) for name in ("FASTQ_1", "FASTQ_2")])
    loaders = {}
    for source in ("batch", "exporter", "fastq_names", "load_names"):
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


def test__main__batch_ignores_all_backlog_inputs(monkeypatch, workflow):
    loaders, manifest = workflow
    monkeypatch.setattr(
        sys,
        "argv",
        (
            "ocs-submission --modality MTX --batch-name-from-vendor MTX-22068 "
            "--ocs-tracker-exporter ignored.csv --fastq-names ignored --load-names ignored "
            "--force-submission alignment --batch-processing true --audit true "
            f"--email {EMAIL} --dry-run true"
        ).split(),
    )

    main.main()

    loaders["batch"].assert_called_once_with("MTX-22068")
    for source in ("exporter", "fastq_names", "load_names"):
        loaders[source].assert_not_called()
    rows = pd.read_json(manifest)
    assert len(rows) == 2
    assert rows["force_submission"].tolist() == ["alignment", "alignment"]
    assert rows["notify_email"].tolist() == [EMAIL, EMAIL]
    assert rows["dry_run"].all()
    main.send_audit_email.assert_not_called()
    main.send_command_summary_email.assert_not_called()


@pytest.mark.parametrize(
    "batch_arguments",
    [
        pytest.param(["--modality", "MTX"], id="modality"),
        pytest.param(["--batch-name-from-vendor", "MTX-22068"], id="vendor_batch"),
        pytest.param(["--force-submission", "alignment"], id="force_submission"),
        pytest.param(["--batch-processing", "true"], id="use_fastq_names"),
        pytest.param(["--audit", "true"], id="audit"),
    ],
)
def test__parse_args__partial_batch_does_not_fall_back_to_backlog(monkeypatch, capsys, batch_arguments):
    monkeypatch.setattr(sys, "argv", ["ocs-submission", *batch_arguments, "--fastq-names", "FASTQ_1"])

    with pytest.raises(SystemExit, match="2"):
        main.parse_args()

    assert "Batch Processing requires --modality and --batch-name-from-vendor" in capsys.readouterr().err


@pytest.mark.parametrize("dry_run", [pytest.param("true", id="dry_run"), pytest.param("false", id="submit")])
@pytest.mark.parametrize(
    "arguments, source, loader_arguments, row_count",
    [
        pytest.param(["--ocs-tracker-exporter", "tracker.csv"], "exporter", "tracker.csv", 2, id="tracker"),
        pytest.param(["--fastq-names", "  FASTQ_1, FASTQ_2  "], "fastq_names", ["FASTQ_1", "FASTQ_2"], 2, id="fastq"),
        pytest.param(["--load-names", "LOAD_1"], "load_names", ["LOAD_1"], 1, id="load"),
        pytest.param(
            ["--ocs-tracker-exporter", "tracker.csv", "--load-names", "ignored", "--fastq-names", "ignored"],
            "exporter",
            "tracker.csv",
            2,
            id="tracker_ignores_other_sources",
        ),
        pytest.param(
            ["--load-names", "LOAD_1", "--fastq-names", "ignored"],
            "load_names",
            ["LOAD_1"],
            1,
            id="load_ignores_fastq_source",
        ),
    ],
)
def test__main__backlog_uses_common_parameters(
    monkeypatch, workflow, arguments, source, loader_arguments, row_count, dry_run
):
    loaders, manifest = workflow
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ocs-submission",
            *arguments,
            "--audit",
            "false",
            "--batch-processing",
            "false",
            "--email",
            EMAIL,
            "--dry-run",
            dry_run,
        ],
    )

    main.main()

    loaders[source].assert_called_once_with(loader_arguments)
    for other_source, loader in loaders.items():
        if other_source != source:
            loader.assert_not_called()
    rows = pd.read_json(manifest)
    assert len(rows) == row_count
    assert rows["modality"].tolist() == ["MTX"] * row_count
    assert rows["notify_email"].tolist() == [EMAIL] * row_count
    assert rows["dry_run"].tolist() == [dry_run == "true"] * row_count
    assert rows["force_submission"].isna().all()
    main.send_audit_email.assert_not_called()
    if dry_run == "true":
        main.send_command_summary_email.assert_not_called()
    else:
        assert main.send_command_summary_email.call_args.kwargs["notify_email"] == EMAIL


def test__main__batch_sends_audit_and_summary_after_submission(monkeypatch, workflow):
    monkeypatch.setattr(
        sys,
        "argv",
        f"ocs-submission --modality MTX --batch-name-from-vendor MTX-22068 --audit true --email {EMAIL}".split(),
    )

    main.main()

    main.send_audit_email.assert_called_once_with("MTX-22068", EMAIL)
    main.send_command_summary_email.assert_called_once()


def test__main__mixed_backlog_modalities_fail_before_submission(monkeypatch, workflow):
    loaders, manifest = workflow
    loaders["fastq_names"].return_value.loc[1, "batch_name_from_vendor"] = "RTX-34056"
    monkeypatch.setattr(sys, "argv", ["ocs-submission", "--fastq-names", "FASTQ_1", "FASTQ_2"])

    with pytest.raises(ValueError, match="Backlog runs require one modality"):
        main.main()

    main.execute_ocs_submission_commands.assert_not_called()
    assert not manifest.exists()
