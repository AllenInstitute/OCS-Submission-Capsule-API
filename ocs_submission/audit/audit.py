#!/usr/bin/env python3
import os
import sys

import pandas as pd
import psycopg2

script_dir = os.path.dirname(os.path.abspath(__file__))


class Rule:
    def __init__(self, columns, condition, ignore=None, tf_values=None):
        if tf_values is None:
            tf_values = ["MISSING", "Present"]
        if isinstance(columns, str):
            columns = [columns]
        self.columns = columns
        self.condition = condition
        self.ignore = ignore
        self.tf_values = tf_values


class Auditor:
    def __init__(self, rules, identifiers):
        self.rules = rules
        self.identifiers = identifiers

    def generate_report(self, dataset):
        missing_data = []
        id_rename = {col: f"{col}_id" for col in self.identifiers}
        id_columns = list(id_rename.values())
        dataset = pd.merge(dataset.rename(columns=id_rename), dataset)

        # Apply rules to the dataset 
        for rule in self.rules:
            rule_output = dataset[rule.columns].apply(rule.condition)
            if callable(rule.ignore):
                rule_output.loc[rule.ignore(dataset), rule.columns] = False
            rule_output = pd.concat(
                [dataset[id_columns], rule_output], join="inner", axis=1
            ).loc[rule_output.any(axis=1)]
            missing_data.append(rule_output)

        # Label the ones that are to be ignored as "Not Required"
        # Label the rest as being present and label the True values as missing
        for rule, rule_output in zip(self.rules, missing_data):
            if rule_output.empty:
                continue
            if callable(rule.ignore):
                rule_output[rule.columns] = rule_output[rule.columns].astype(object)
                rule_output.loc[rule.ignore(dataset), rule.columns] = "Not required"
            rule_output.loc[:, rule.columns] = rule_output.loc[:, rule.columns].replace(
                {True: rule.tf_values[0], False: rule.tf_values[1]}
            )

        # Concatenate the missing data and reset the index
        missing_data = pd.concat(missing_data).reset_index(drop=True)

        # Fill the missing values with "Present"
        missing_data.loc[:, ~missing_data.columns.str.endswith("_id")] = (
            missing_data.loc[:, ~missing_data.columns.str.endswith("_id")].fillna(
                "Present"
            )
        )
        return missing_data


class RTXAuditor(Auditor):
    def __init__(self):
        identifiers = ["batch_vendor_name", "sample_name", "species", "studies", "external_donor_name", "donor_name"]
        rules = [
            Rule(
                "full_genotype",
                lambda col: pd.isna(col) | col.str.contains("NULL", na=False),
                lambda df: df["organism"] != "mouse"
            ),
            Rule(
                ["injection_method", "injection_roi", "injection_materials"],
                pd.isna,
                lambda df: (
                    df[["injection_method", "injection_roi", "injection_materials"]].isna().all(axis=1) &
                    ~df["studies"].str.contains("Enhancer|Zirong|Nelson", na=False) &
                    ~df["studies"].isin([
                        "HGT", "Barcoded_Rabies_Virus", "ZhigangHe", "RetroSeq",
                        "Viral_Reporter_Local", "Nowakowski_Dev", "HGT_Enhancer_Virus"
                    ])
                )
            ),
            Rule(
                "age",
                lambda col: col.str.lower() == "unknown",
                tf_values=["UNKNOWN (flagged for review - age may be genuinely unknown, or updated later)", "Present"]
            ),
            Rule(
                ["facs_population_plan", "age", "sex", "sample_name", "load_name", "studies", "roi"],
                pd.isna
            ),
        ]
        super().__init__(rules, identifiers)


class MTXAuditor(Auditor):
    def __init__(self):
        identifiers = ["batch_vendor_name", "sample_name", "species", "studies", "external_donor_name", "donor_name"]
        rules = [
            Rule(
                "full_genotype",
                pd.isna,
                lambda df: df["organism"] != "mouse"
            ),
            Rule(
                ["injection_method", "injection_roi", "injection_materials"],
                pd.isna,
                lambda df: df[["injection_method", "injection_roi", "injection_materials"]].isna().all(axis=1)
            ),
            Rule(
                "age",
                lambda col: col.str.lower() == "unknown",
                tf_values=["UNKNOWN (flagged for review - age may be genuinely unknown, or updated later", "Present"]
            ),
            Rule(
                ["facs_population_plan", "age", "sex", "sample_name", "load_name", "studies", "roi"],
                pd.isna
            ),
        ]
        super().__init__(rules, identifiers)


def run_audit(batch_name_from_vendor: str) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """Pull LIMS data for ``batch_name_from_vendor`` and build the missing-data report.

    Returns ``(lims_data, report, modality)``. The caller decides what to do with the
    CSVs (e.g. email them).
    """
    prefix = batch_name_from_vendor.split("-")[0][:3]

    sql_file = f"{script_dir}/lims_rtx_ocs.sql" if prefix in ("RTX", "10X") else f"{script_dir}/lims_mtx_ocs.sql"
    auditor = RTXAuditor() if prefix in ("RTX", "10X") else MTXAuditor()
    modality = prefix if prefix in ("RTX", "10X") else "MTX"

    conn = psycopg2.connect(
        host="lims2.private-allenneuraldynamics.org",
        database="lims2",
        user=os.environ["DATABASE_USERNAME"],
        password=os.environ["DATABASE_PASSWORD"],
    )

    with open(sql_file) as f:
        sql = f.read().format(
            load_name="''",
            exp_component_name="''",
            batch_name=f"'{batch_name_from_vendor}'",
        )

    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [d[0] for d in cur.description]
    conn.close()

    lims_data = pd.DataFrame(rows, columns=columns)
    report = auditor.generate_report(lims_data)

    return lims_data, report, modality


if __name__ == "__main__":
    lims_data, report, modality = run_audit(sys.argv[1])
    print(report.to_csv(index=False))
