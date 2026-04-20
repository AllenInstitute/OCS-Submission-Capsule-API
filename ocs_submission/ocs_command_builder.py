"""Build OCS job command records from FASTQ records and workflow templates."""

import pandas as pd

COMMAND_RECORD_COLUMNS = [
    "fastq_name",
    "study_set",
    "load_name",
    "library_prep_method_name",
    "organism_common_name",
    "batch_name_from_vendor",
    "modality",
    "ingest_status",
    "alignment_status",
    "post_alignment_status",
    "force_submission",
    "dry_run",
    "notify_email",
    "alignment_should_execute",
    "alignment_command_args",
    "alignment_command",
    "alignment_spacing",
    "alignment_demand_id",
    "alignment_submission_success",
    "alignment_error_message",
    "alignment_output",
    "alignment_executed_at",
    "post_alignment_should_execute",
    "post_alignment_command_args",
    "post_alignment_command",
    "post_alignment_spacing",
    "post_alignment_demand_id",
    "post_alignment_submission_success",
    "post_alignment_error_message",
    "post_alignment_output",
    "post_alignment_executed_at",
]


def select_alignment_command_config(
    config: dict,
    modality: str,
    library_prep_method_name: str,
    organism_common_name: str,
) -> dict:
    """
    Pick the alignment command config that matches a FASTQ's metadata.

    Configs are checked in list order and the first match wins. A config matches when its library
    prep rule and organism rule each contain the FASTQ's value, or the wildcard ``"*"``.

    Parameters
    ----------
    config
        Application configuration containing ``workflows[modality]["alignment_command_configs"]``.
    modality
        Workflow modality, such as ``RTX``, ``MTX``, or ``RFX``.
    library_prep_method_name
        Library prep method name from the FASTQ metadata.
    organism_common_name
        Organism common name from the FASTQ metadata.

    Return
    ----------
    dict
        The first matching alignment command config.

    Pseudo code
    ----------
    read the ordered list of alignment command configs for the modality
    for each config entry:
        check whether the library prep matches
        check whether the organism matches
        return the first matching config entry
    raise an error if no config matches
    """
    alignment_command_configs = config["workflows"][modality][
        "alignment_command_configs"
    ]

    for command_config in alignment_command_configs:
        match = command_config.get("match", {})
        library_preps = match.get("library_preps", ["*"])
        organisms = match.get("organisms", ["*"])

        library_prep_matches = (
            "*" in library_preps or library_prep_method_name in library_preps
        )
        organism_matches = "*" in organisms or organism_common_name in organisms

        if library_prep_matches and organism_matches:
            return command_config

    raise ValueError(
        f"No {modality} alignment command config found for {library_prep_method_name}"
    )


def build_ocs_command_args(
    config: dict,
    fastq_record,
    modality: str,
    email: str,
    command_template: dict,
) -> tuple[list[str], int]:
    """
    Render an OCS command template into argv-style command arguments.

    Starts from the template's base command, then appends each configured argument. Values are
    filled in with ``str.format`` using ``command_template_field_values`` — the map from template
    field name (``reference_name``, ``chemistry``, ...) to the resolved value for this
    ``fastq_record``.

    Parameters
    ----------
    config
        Application configuration with chemistry, probe set, and reference mappings.
    fastq_record
        FASTQ record being used to build the command.
    modality
        Workflow modality, such as ``RTX``, ``MTX``, or ``RFX``.
    email
        Notification email to include in the command arguments.
    command_template
        Command template containing the base command, arguments, spacing, and optional
        ``execution_vcpus``.

    Return
    ----------
    tuple[list[str], int]
        Tuple of ``(command_args, spacing)`` where ``command_args`` is the argv-style command list
        and ``spacing`` is the configured delay before the next submission.

    Pseudo code
    ----------
    build command_template_field_values (field name → value for this fastq)
    copy the base command from the template
    for each configured argument:
        append the flag
        append the rendered value when one exists
    return the command arguments and spacing
    """
    library_prep_method_name = fastq_record.library_prep_method_name
    organism_common_name = fastq_record.organism_common_name
    chemistry_by_library_prep = config["chemistry_by_library_prep"]
    probe_sets_by_organism = config["probe_sets_by_organism"]
    organism_references = config["references"][organism_common_name]
    if modality in organism_references:
        reference_name = organism_references[modality]
    else:
        reference_name = organism_references["all"]
    command_template_field_values = {
        "reference_name": reference_name,
        "load_name": fastq_record.load_name,
        "email": email,
        "chemistry": chemistry_by_library_prep.get(library_prep_method_name, ""),
        "probe_set": probe_sets_by_organism.get(organism_common_name, {}).get(
            library_prep_method_name, ""
        ),
        "execution_vcpus": command_template.get("execution_vcpus", ""),
    }

    command_args = list(command_template["command"])
    for argument in command_template["arguments"]:
        command_args.append(argument["flag"])
        if "value" in argument:
            command_args.append(argument["value"].format(**command_template_field_values))

    return command_args, command_template["spacing"]


def build_alignment_job_command_record(
    fastq_record,
    modality: str,
    config: dict,
    email: str,
    force_submission: str | None,
    dry_run: bool,
) -> dict:
    """
    Build the alignment job command record for one FASTQ.

    Ingest and current alignment status decide whether alignment should run. When it should, the
    helper picks the alignment command config for this FASTQ's modality, library prep, and
    organism, and renders the concrete command arguments.

    Parameters
    ----------
    fastq_record
        FASTQ record containing metadata and current stage statuses.
    modality
        Workflow modality, such as ``RTX``, ``MTX``, or ``RFX``.
    config
        Application configuration with status mappings and workflow command configs.
    email
        Notification email to store on the command record.
    force_submission
        ``"alignment"`` to force alignment submission, or ``None``.
    dry_run
        Whether this run is a dry run.

    Return
    ----------
    dict
        One command record dictionary for an alignment job.

    Pseudo code
    ----------
    decide whether alignment should execute based on ingest and alignment status
    if alignment should execute:
        select the matching alignment command config
        build the concrete command arguments
    return the full alignment command record
    """
    ingest_complete_statuses = config["status_mappings"]["ingest_complete"]
    alignment_complete_statuses = config["status_mappings"]["alignment_complete"]

    ingest_status = fastq_record.ingest_status
    alignment_status = fastq_record.alignment_status

    should_execute = False
    command_args = None
    spacing = None

    if ingest_status not in ingest_complete_statuses:
        pass
    elif force_submission == "alignment":
        should_execute = True
    elif alignment_status in alignment_complete_statuses:
        pass
    elif alignment_status == "IN_PROGRESS":
        pass
    else:
        should_execute = True

    if should_execute:
        alignment_command_config = select_alignment_command_config(
            config=config,
            modality=modality,
            library_prep_method_name=fastq_record.library_prep_method_name,
            organism_common_name=fastq_record.organism_common_name,
        )
        command_args, spacing = build_ocs_command_args(
            config=config,
            fastq_record=fastq_record,
            modality=modality,
            email=email,
            command_template=alignment_command_config,
        )

    return {
        "alignment_should_execute": should_execute,
        "alignment_command_args": command_args,
        "alignment_command": " ".join(command_args) if command_args else None,
        "alignment_spacing": spacing,
        "alignment_demand_id": None,
        "alignment_submission_success": None,
        "alignment_error_message": None,
        "alignment_output": None,
        "alignment_executed_at": None,
    }


def build_post_alignment_job_command_record(
    fastq_record,
    modality: str,
    config: dict,
    email: str,
    force_submission: str | None,
    dry_run: bool,
    alignment_should_execute: bool,
) -> dict:
    """
    Build the post-alignment job command record for one FASTQ.

    Post-alignment runs only after alignment is complete and not already in progress or done,
    unless ``force_submission`` overrides that. If alignment is scheduled to submit in this same
    pass, post-alignment is deferred until the next run.

    Parameters
    ----------
    fastq_record
        FASTQ record containing metadata and current stage statuses.
    modality
        Workflow modality, such as ``RTX``, ``MTX``, or ``RFX``.
    config
        Application configuration with status mappings and the post-alignment template.
    email
        Notification email to store on the command record.
    force_submission
        ``"post-alignment"`` to force submission, or ``None``.
    dry_run
        Whether this run is a dry run.
    alignment_should_execute
        Whether alignment is scheduled to submit for this FASTQ in the current run.

    Return
    ----------
    dict
        One command record dictionary for a post-alignment job.

    Pseudo code
    ----------
    decide whether post-alignment should execute
    if post-alignment should execute:
        build the concrete command arguments from the post-alignment template
    return the full post-alignment command record
    """
    alignment_complete_statuses = config["status_mappings"]["alignment_complete"]
    post_alignment_complete_statuses = config["status_mappings"][
        "post_alignment_complete"
    ]

    alignment_status = fastq_record.alignment_status
    post_alignment_status = fastq_record.post_alignment_status

    should_execute = False
    command_args = None
    spacing = None

    if alignment_should_execute:
        pass
    elif alignment_status not in alignment_complete_statuses:
        pass
    elif force_submission == "post-alignment":
        should_execute = True
    elif post_alignment_status in post_alignment_complete_statuses:
        pass
    elif post_alignment_status == "IN_PROGRESS":
        pass
    else:
        should_execute = True

    if should_execute:
        command_args, spacing = build_ocs_command_args(
            config=config,
            fastq_record=fastq_record,
            modality=modality,
            email=email,
            command_template=config["workflows"][modality]["post_alignment"],
        )

    return {
        "post_alignment_should_execute": should_execute,
        "post_alignment_command_args": command_args,
        "post_alignment_command": " ".join(command_args) if command_args else None,
        "post_alignment_spacing": spacing,
        "post_alignment_demand_id": None,
        "post_alignment_submission_success": None,
        "post_alignment_error_message": None,
        "post_alignment_output": None,
        "post_alignment_executed_at": None,
    }


def build_ocs_job_submission_command(
    fastq_records_df: pd.DataFrame,
    modality: str,
    config: dict,
    email: str,
    force_submission: str | None,
    dry_run: bool,
) -> pd.DataFrame:
    """
    Build the command records dataframe for every FASTQ in the input frame.

    Each FASTQ produces two rows: one alignment record and one post-alignment record. The
    post-alignment builder is told whether alignment is about to run in this same pass so it can
    skip when running both out of order would be wrong.

    Parameters
    ----------
    fastq_records_df
        Input dataframe of FASTQ records.
    modality
        Workflow modality, such as ``RTX``, ``MTX``, or ``RFX``.
    config
        Application configuration.
    email
        Notification email to include on command records.
    force_submission
        Optional forced submission stage.
    dry_run
        Whether this run is a dry run.

    Return
    ----------
    pd.DataFrame
        Dataframe of command records with ``COMMAND_RECORD_COLUMNS``.

    Pseudo code
    ----------
    start an empty list of command rows
    for each FASTQ record:
        build the alignment job command record
        build the post-alignment job command record
        append both records
    return the rows as a dataframe in standard column order
    """
    command_rows = []

    for fastq_record in fastq_records_df.itertuples(index=False):
        alignment_record = build_alignment_job_command_record(
            fastq_record=fastq_record,
            modality=modality,
            config=config,
            email=email,
            force_submission=force_submission,
            dry_run=dry_run,
        )

        post_alignment_record = build_post_alignment_job_command_record(
            fastq_record=fastq_record,
            modality=modality,
            config=config,
            email=email,
            force_submission=force_submission,
            dry_run=dry_run,
            alignment_should_execute=alignment_record["alignment_should_execute"],
        )

        shared_record = {
            "fastq_name": fastq_record.fastq_name,
            "study_set": fastq_record.study_set,
            "load_name": fastq_record.load_name,
            "library_prep_method_name": fastq_record.library_prep_method_name,
            "organism_common_name": fastq_record.organism_common_name,
            "batch_name_from_vendor": fastq_record.batch_name_from_vendor,
            "modality": modality,
            "ingest_status": fastq_record.ingest_status,
            "alignment_status": fastq_record.alignment_status,
            "post_alignment_status": fastq_record.post_alignment_status,
            "force_submission": force_submission,
            "dry_run": dry_run,
            "notify_email": email,
        }

        command_rows.append({**shared_record, **alignment_record, **post_alignment_record})

    return pd.DataFrame(command_rows, columns=COMMAND_RECORD_COLUMNS)
