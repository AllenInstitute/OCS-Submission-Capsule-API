"""Convert an OCS JSONC configuration into three Markdown tables."""

import argparse
import logging
from html import escape
from pathlib import Path

from ..commands.builder import select_reference_name
from .loader import load_jsonc_config


def markdown_table(title: str, headers: list[str], rows: list[list[str]]) -> str:
    """Render a titled Markdown table from text cells.

    Escape HTML and pipe characters, and preserve cell line breaks with HTML breaks.

    Parameters:
    title: Heading above the table.
    headers: Column labels in display order.
    rows: Text cells in the same order as the headers.

    Returns:
    Markdown text ending with a newline.
    """
    lines = [f"# {title}", "", " | ".join(headers), " | ".join("---" for _ in headers)]
    for row in rows:
        lines.append(" | ".join(escape(cell).replace("|", "&#124;").replace("\n", "<br>") for cell in row))
    return "\n".join(lines) + "\n"


def command_rows(config: dict, stage: str) -> list[list[str]]:
    """Summarize command templates for one workflow stage.

    Include asset versions, input identifiers, options, and organism restrictions.
    Expand chemistry mappings by library prep when a template uses them.

    Parameters:
    config: Parsed OCS workflow configuration.
    stage: Either 'alignment' or 'post_alignment'.

    Returns:
    Table rows in workflow and template order.
    """
    rows = []
    for modality, workflow in config["workflows"].items():
        for template in workflow[f"{stage}_command_configs"]:
            arguments = {argument["flag"]: argument.get("value", "") for argument in template["arguments"]}
            library_preps = template["match"]["library_preps"]
            options = []
            for argument in template["arguments"]:
                if argument["flag"] in {
                    "--asset-name",
                    "--asset-tag",
                    "--load-names",
                    "--fastq-names",
                    "--reference-names",
                    "--notify",
                    "--notify-on",
                }:
                    continue
                value = argument.get("value", "")
                value = value.replace("{execution_vcpus}", str(template.get("execution_vcpus", "")))
                options.append(f"{argument['flag']} {value}".strip())
            if any("{chemistry}" in option for option in options):
                options.append(
                    "Chemistry: "
                    + "; ".join(
                        f"{prep}: {config['chemistry_by_library_prep'].get(prep) or 'Missing'}"
                        for prep in library_preps
                    )
                )
            if "organisms" in template["match"]:
                options.append("Organisms: " + "; ".join(template["match"]["organisms"]))
            identifiers = []
            if modality == "RTX" and stage == "post_alignment":
                identifiers.append("Fastq name")
            else:
                if "--load-names" in arguments:
                    identifiers.append("Load name")
                if "--fastq-names" in arguments:
                    identifiers.append("Fastq name")
            rows.append(
                [
                    modality,
                    " ".join(template["command"]),
                    arguments.get("--asset-name", "Default"),
                    arguments.get("--asset-tag", "Default"),
                    "; ".join(library_preps),
                    "; ".join(identifiers),
                    "\n".join(options),
                ]
            )
    return rows


def reference_rows(config: dict) -> list[list[str]]:
    """Summarize references and required probe sets for matching alignment templates.

    Use the first matching template for each organism, modality, and library prep.
    Group identical settings and report missing reference or probe mappings in the rows.

    Parameters:
    config: Parsed OCS configuration with expanded organism reference keys.

    Returns:
    Table rows containing organisms, modalities, library preps, mappings, and status notes.
    """
    rows = []
    for organism in config["references"]:
        for modality, workflow in config["workflows"].items():
            grouped: dict[tuple[str, ...], list[str]] = {}
            matched_preps = set()
            for template in workflow["alignment_command_configs"]:
                if organism not in template["match"].get("organisms", [organism]):
                    continue
                requires_probe = any(
                    "{probe_set}" in argument.get("value", "") or "{probe_set}" in argument["flag"]
                    for argument in template["arguments"]
                )
                for prep in template["match"]["library_preps"]:
                    # Command selection uses the first matching template.
                    if prep in matched_preps:
                        continue
                    matched_preps.add(prep)
                    notes = []
                    try:
                        reference = select_reference_name(config, modality, organism, prep)
                    except KeyError as error:
                        reference = ""
                        notes.append(error.args[0])
                    probe_config = config["probe_sets_by_organism"].get(organism, {})
                    probe = probe_config if isinstance(probe_config, str) else probe_config.get(prep, "")
                    missing = []
                    if not reference:
                        missing.append("Missing reference mapping")
                    if requires_probe and not probe:
                        missing.append("Missing probe mapping")
                        notes.append("The alignment template requires {probe_set}, but no matching probe set exists.")
                    key = (
                        reference,
                        probe if requires_probe else "",
                        "Required" if requires_probe else "Not required",
                        "; ".join(missing) or "Configured",
                        "\n".join(notes),
                    )
                    grouped.setdefault(key, []).append(prep)
            for (reference, probe, requirement, status, note_text), preps in grouped.items():
                rows.append([organism, modality, "; ".join(preps), reference, probe, requirement, status, note_text])

    # Combine organism aliases only when all displayed settings agree.
    organisms_by_row: dict[tuple[str, ...], list[str]] = {}
    for organism, *settings in rows:
        organisms_by_row.setdefault(tuple(settings), []).append(organism)
    return [["; ".join(organisms), *settings] for settings, organisms in organisms_by_row.items()]


def main() -> None:
    """Read a JSONC input file and write three Markdown reports.

    Create the output directory if needed and overwrite reports with the same names.
    Log each written path. File access and configuration parsing errors propagate.

    Parameters:
    None. Read the input file and output directory from command-line arguments.

    Returns:
    None.
    """
    parser = argparse.ArgumentParser(description="Export references, alignment, and post-alignment Markdown tables.")
    parser.add_argument("--input", "--config", required=True, help="Source JSONC file.")
    parser.add_argument("--output-dir", type=Path, required=True, help="Directory for the three Markdown files.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    config = load_jsonc_config(args.input)
    command_headers = [
        "Modality",
        "Command",
        "Asset",
        "Tag",
        "Library Prep Methods",
        "Input Identifier",
        "Distinguishing Options",
    ]
    tables = {
        "references.md": markdown_table(
            "References and probe sets",
            [
                "Organism",
                "Modality",
                "Library Prep Methods",
                "Reference Genome / OCS Reference ID",
                "Probe Set / OCS Probe Reference ID",
                "Probe Requirement",
                "Configuration Status",
                "Notes",
            ],
            reference_rows(config),
        ),
        "alignment.md": markdown_table("Alignment commands", command_headers, command_rows(config, "alignment")),
        "post_alignment.md": markdown_table(
            "Post-alignment commands",
            command_headers,
            command_rows(config, "post_alignment"),
        ),
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for filename, content in tables.items():
        path = args.output_dir / filename
        path.write_text(content, encoding="utf-8")
        logging.info("Wrote %s", path)


if __name__ == "__main__":
    main()
