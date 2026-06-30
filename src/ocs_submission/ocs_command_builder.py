"""Build OCS submission commands from fastq samples and workflow templates."""

import pandas as pd

from .stages import Stage

COMMAND_CONFIG_BY_STAGE = {
    Stage.ALIGNMENT: ("alignment_command_configs", "alignment"),
    Stage.POST_ALIGNMENT: ("post_alignment_command_configs", "post-alignment"),
}

JOB_RECORD_FIELDS = (
    "should_execute",
    "command_args",
    "command",
    "spacing",
    "demand_id",
    "submission_success",
    "error_message",
    "executed_at",
)

COMMAND_RECORD_COLUMNS = [
    "fastq_name",
    "study_set",
    "load_name",
    "library_prep_method_name",
    "organism_common_name",
    "batch_name_from_vendor",
    "modality",
    *(stage.fastq_status_column for stage in Stage),
    "force_submission",
    "dry_run",
    "notify_email",
    *(
        f"{stage.ocs_stage_name}_{field}"
        for stage in (Stage.ALIGNMENT, Stage.POST_ALIGNMENT)
        for field in JOB_RECORD_FIELDS
    ),
]


def select_command_config(
    config: dict,
    modality: str,
    stage: Stage,
    library_prep_method_name: str,
    organism_common_name: str,
) -> dict:
    """
    Pick the first command template that matches a fastq sample's stage, library prep,
    and organism.

    Parameters:
    config: The OCS workflow configuration.
    modality: The modality (RTX, MTX, RFX) to look up templates for.
    stage: The OCS stage to look up templates for.
    library_prep_method_name: The sample's library prep method.
    organism_common_name: The sample's organism.

    Returns:
    The matching command template from the config. If nothing matches, an error is raised.
    """
    workflow = config["workflows"][modality]
    command_config_field, command_config_label = COMMAND_CONFIG_BY_STAGE[stage]
    command_configs = workflow[command_config_field]

    for command_config in command_configs:
        try:
            match = command_config["match"]
            library_preps = match["library_preps"]
        except KeyError as error:
            raise KeyError("library_preps not listed in the config file") from error

        organisms = match.get("organisms")

        # Omit organisms in config to match any organism.
        if library_prep_method_name in library_preps and (organisms is None or organism_common_name in organisms):
            return command_config

    raise ValueError(f"No {modality} {command_config_label} command config found for {library_prep_method_name}")


def build_ocs_command_args(
    config: dict,
    fastq_record,
    modality: str,
    email: str,
    command_template: dict,
) -> tuple[list[str], int]:
    """
    Fill in a command template for one fastq sample and return the command to run.

    Parameters:
    config: The OCS workflow configuration.
    fastq_record: The fastq sample whose metadata is substituted into the command.
    modality: The modality used to look up the reference genome.
    email: The notification email address for OCS.
    command_template: The base command, arguments, and wait time from the config.

    Returns:
    The command as a list of strings, and how many seconds to wait before submitting the
    next job. If no reference is configured for the sample's organism and modality, an
    error is raised.
    """
    library_prep_method_name = fastq_record.library_prep_method_name
    organism_common_name = fastq_record.organism_common_name
    chemistry_by_library_prep = config["chemistry_by_library_prep"]
    probe_sets_by_organism = config["probe_sets_by_organism"]
    organism_references = config["references"][organism_common_name]
    if modality in organism_references:
        reference_name = organism_references[modality]
    elif "all" in organism_references:
        reference_name = organism_references["all"]
    else:
        raise KeyError(
            f"No reference for organism {organism_common_name!r} with modality {modality!r}: "
            f"expected a {modality!r} or 'all' entry in "
            f"config['references'][{organism_common_name!r}], "
            f"found keys {sorted(organism_references)}"
        )
    command_template_field_values = {
        "reference_name": reference_name,
        "load_name": fastq_record.load_name,
        "email": email,
        "chemistry": chemistry_by_library_prep.get(library_prep_method_name, ""),
        "probe_set": probe_sets_by_organism.get(organism_common_name, {}).get(library_prep_method_name, ""),
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
) -> dict:
    """
    Decide whether to run alignment for one fastq sample and build the command if needed.

    Alignment runs when ingest is complete and alignment has not finished or started yet.
    It also runs when the user forces alignment with force_submission.

    Parameters:
    fastq_record: The fastq sample and its ingest and alignment statuses.
    modality: The modality used to pick the alignment template.
    config: The OCS workflow configuration.
    email: The notification email address for OCS.
    force_submission: Set to "alignment" to run alignment even if it would normally be skipped.

    Returns:
    Alignment fields for one row of the submission manifest. Command fields are empty when
    alignment is not scheduled.
    """
    ingest_complete_statuses = config["status_mappings"]["ingest_complete"]
    align_complete_statuses = config["status_mappings"]["alignment_complete"]

    ingest_status = fastq_record.ingest_status
    align_status = fastq_record.align_status

    should_execute = False
    command_args = None
    spacing = None

    if ingest_status in ingest_complete_statuses and (
        force_submission == "alignment"
        or (align_status not in align_complete_statuses and align_status != "IN_PROGRESS")
    ):
        should_execute = True

    if should_execute:
        align_command_config = select_command_config(
            config=config,
            modality=modality,
            stage=Stage.ALIGNMENT,
            library_prep_method_name=fastq_record.library_prep_method_name,
            organism_common_name=fastq_record.organism_common_name,
        )
        command_args, spacing = build_ocs_command_args(
            config=config,
            fastq_record=fastq_record,
            modality=modality,
            email=email,
            command_template=align_command_config,
        )

    return {
        "align_should_execute": should_execute,
        "align_command_args": command_args,
        "align_command": " ".join(command_args) if command_args else None,
        "align_spacing": spacing,
        "align_demand_id": None,
        "align_submission_success": None,
        "align_error_message": None,
        "align_executed_at": None,
    }


def build_post_alignment_job_command_record(
    fastq_record,
    modality: str,
    config: dict,
    email: str,
    force_submission: str | None,
    alignment_should_execute: bool,
) -> dict:
    """
    Decide whether to run post-alignment for one fastq sample and build the command if needed.

    Parameters:
    fastq_record: The fastq sample and its alignment and post-alignment statuses.
    modality: The modality used to pick the post-alignment template.
    config: The OCS workflow configuration.
    email: The notification email address for OCS.
    force_submission: Set to "post-alignment" to run post-alignment even if it would normally
        be skipped.
    alignment_should_execute: Whether alignment is scheduled in the same pass.

    Returns:
    Post-alignment fields for one row of the submission manifest. Command fields are empty
    when post-alignment is not scheduled.
    """
    align_complete_statuses = config["status_mappings"]["alignment_complete"]
    postalign_complete_statuses = config["status_mappings"]["post_alignment_complete"]

    align_status = fastq_record.align_status
    postalign_status = fastq_record.postalign_status
    postalign_template = select_command_config(
        config=config,
        modality=modality,
        stage=Stage.POST_ALIGNMENT,
        library_prep_method_name=fastq_record.library_prep_method_name,
        organism_common_name=fastq_record.organism_common_name,
    )

    should_execute = False
    command_args = None
    spacing = None

    if (
        not alignment_should_execute
        and align_status in align_complete_statuses
        and (
            force_submission == "post-alignment"
            or (postalign_status not in postalign_complete_statuses and postalign_status != "IN_PROGRESS")
        )
    ):
        should_execute = True
        command_args, spacing = build_ocs_command_args(
            config=config,
            fastq_record=fastq_record,
            modality=modality,
            email=email,
            command_template=postalign_template,
        )

    return {
        "postalign_should_execute": should_execute,
        "postalign_command_args": command_args,
        "postalign_command": " ".join(command_args) if command_args else None,
        "postalign_spacing": spacing,
        "postalign_demand_id": None,
        "postalign_submission_success": None,
        "postalign_error_message": None,
        "postalign_executed_at": None,
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
    Build the full submission manifest with one row per fastq sample.

    Parameters:
    fastq_records_df: A dataframe of fastq samples, one row per sample.
    modality: The modality to use when building commands.
    config: The OCS workflow configuration.
    email: The notification email address recorded on each row.
    force_submission: Optionally force alignment or post-alignment to run.
    dry_run: Whether this run is a dry run (recorded on each row).

    Returns:
    A dataframe ready for submission, with one row per fastq sample.
    """
    command_row_list = list()

    for fastq_record in fastq_records_df.itertuples(index=False):
        alignment_record = build_alignment_job_command_record(
            fastq_record=fastq_record,
            modality=modality,
            config=config,
            email=email,
            force_submission=force_submission,
        )

        postalign_record = build_post_alignment_job_command_record(
            fastq_record=fastq_record,
            modality=modality,
            config=config,
            email=email,
            force_submission=force_submission,
            alignment_should_execute=alignment_record["align_should_execute"],
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
            "align_status": fastq_record.align_status,
            "postalign_status": fastq_record.postalign_status,
            "force_submission": force_submission,
            "dry_run": dry_run,
            "notify_email": email,
        }

        command_row_list.append({**shared_record, **alignment_record, **postalign_record})

    return pd.DataFrame(command_row_list, columns=COMMAND_RECORD_COLUMNS)
