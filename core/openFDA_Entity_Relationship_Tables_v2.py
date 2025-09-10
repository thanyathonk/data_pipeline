#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Optimized ER table builder for openFDA drug event data.

Goals:
- Preserve output parity with core/openFDA_Entity_Relationship_Tables.py
- Improve performance via targeted column reads, fewer sorts, vectorized ops
- Provide clear progress logging and optional progress bars

Outputs (written under data/openFDA_drug_event/er_tables/):
- report.csv.gz
- report_serious.csv.gz
- reporter.csv.gz
- patient.csv.gz
- drugcharacteristics.csv.gz
- drugs.csv.gz
- reactions.csv.gz
- standard_drugs.csv.gz
- standard_reactions.csv.gz
- standard_drugs_atc.csv.gz
- standard_drugs_rxnorm_ingredients.csv.gz
- standard_reactions_meddra_relationships.csv.gz
- standard_reactions_meddra_hlt.csv.gz
- standard_reactions_meddra_hlgt.csv.gz
- standard_reactions_meddra_soc.csv.gz
- standard_reactions_snomed.csv.gz

Notes:
- This script uses only the Python standard library and pandas/numpy to avoid
  dependency on extra packages at runtime. If `tqdm` is available, progress
  bars are shown; otherwise, logging messages are used.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import gzip
import logging
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

import numpy as np
import pandas as pd


# ------------------------------
# Config / Logging
# ------------------------------

PRIMARY_KEY = "safetyreportid"


def setup_logging(verbosity: int = 1) -> None:
    level = logging.INFO if verbosity <= 1 else logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )


def maybe_tqdm(iterable: Iterable, desc: str):
    try:
        from tqdm import tqdm  # type: ignore

        return tqdm(iterable, desc=desc)
    except Exception:
        return iterable


# ------------------------------
# Helpers
# ------------------------------


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_csv_fast(
    path: Path,
    usecols: Optional[Sequence[str]] = None,
    dtype: Optional[dict] = None,
    compression: Optional[str] = None,
) -> pd.DataFrame:
    try:
        df = pd.read_csv(
            path,
            usecols=usecols,
            dtype=dtype,
            compression=compression,
            low_memory=False,
        )
    except ValueError:
        # Some shards may be missing optional columns; fall back to full read then subset
        df = pd.read_csv(
            path,
            dtype=dtype,
            compression=compression,
            low_memory=False,
        )
        if usecols is not None:
            keep = [c for c in usecols if c in df.columns]
            df = df.loc[:, keep]
    return df


def concat_files(
    files: List[Path],
    usecols: Optional[Sequence[str]] = None,
    dtype: Optional[dict] = None,
    compression: Optional[str] = "gzip",
    workers: int = 4,
    desc: str = "",
    max_files: Optional[int] = None,
) -> pd.DataFrame:
    if not files:
        return pd.DataFrame(columns=usecols or [])

    if max_files is not None:
        files = files[: max(0, int(max_files))]

    logging.info(f"Reading {len(files)} files: {desc}")
    parts: List[pd.DataFrame] = []

    # Thread pool is effective for I/O and gzip decompression
    with cf.ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futs = [
            ex.submit(read_csv_fast, f, usecols=usecols, dtype=dtype, compression=compression)
            for f in files
        ]
        for fut in maybe_tqdm(cf.as_completed(futs), desc=f"load:{desc}"):
            parts.append(fut.result())

    if parts:
        out = pd.concat(parts, axis=0, ignore_index=True, sort=False)
    else:
        out = pd.DataFrame(columns=usecols or [])
    return out


def write_csv_gz(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    logging.info(f"Writing: {path}")
    df.to_csv(path, index=False, compression="gzip")


def sort_cols(df: pd.DataFrame) -> pd.DataFrame:
    # Match original behavior: columns sorted lexicographically
    return df.reindex(sorted(df.columns), axis=1)


def to_str(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = df[col].astype(str)


# ------------------------------
# Pipeline Steps
# ------------------------------


def step_report(data_dir: Path, er_dir: Path, workers: int, max_files: Optional[int]) -> None:
    dir_ = data_dir / "report"
    files = sorted(dir_.glob("*.csv.gzip"))
    usecols = [
        PRIMARY_KEY,
        "receiptdate",
        "receivedate",
        "transmissiondate",
        "serious",
        "seriousnesscongenitalanomali",
        "seriousnesslifethreatening",
        "seriousnessdisabling",
        "seriousnessdeath",
        "seriousnessother",
        "companynumb",
        "primarysource.qualification",
        "primarysource.reportercountry",
    ]
    dtype = {PRIMARY_KEY: "str"}
    df = concat_files(files, usecols=usecols, dtype=dtype, workers=workers, desc="report", max_files=max_files)
    to_str(df, PRIMARY_KEY)

    # report.csv.gz: groupby max on dates
    rep_cols = [PRIMARY_KEY, "receiptdate", "receivedate", "transmissiondate"]
    report_er = (
        df.loc[:, rep_cols]
        .rename(
            columns={
                "receiptdate": "mostrecent_receive_date",
                "receivedate": "receive_date",
                "transmissiondate": "lastupdate_date",
            }
        )
        .dropna(subset=[PRIMARY_KEY])
    )
    report_er = sort_cols(report_er)
    # Match original: groupby max, reset index
    report_out = report_er.groupby(PRIMARY_KEY, as_index=False).agg("max").dropna(subset=[PRIMARY_KEY])
    write_csv_gz(report_out, er_dir / "report.csv.gz")

    # report_serious.csv.gz: first per primary key
    ser_cols = [
        PRIMARY_KEY,
        "serious",
        "seriousnesscongenitalanomali",
        "seriousnesslifethreatening",
        "seriousnessdisabling",
        "seriousnessdeath",
        "seriousnessother",
    ]
    serious_er = (
        df.loc[:, ser_cols]
        .rename(
            columns={
                "seriousnesscongenitalanomali": "congenital_anomali",
                "seriousnesslifethreatening": "life_threatening",
                "seriousnessdisabling": "disabling",
                "seriousnessdeath": "death",
                "seriousnessother": "other",
            }
        )
        .dropna(subset=[PRIMARY_KEY])
    )
    serious_er = (
        serious_er.sort_values(by=[PRIMARY_KEY]).drop_duplicates(subset=[PRIMARY_KEY], keep="first")
    )
    serious_er = sort_cols(serious_er)
    write_csv_gz(serious_er, er_dir / "report_serious.csv.gz")

    # reporter.csv.gz: first per primary key
    rep_cols = [
        PRIMARY_KEY,
        "companynumb",
        "primarysource.qualification",
        "primarysource.reportercountry",
    ]
    reporter = (
        df.loc[:, rep_cols]
        .rename(
            columns={
                "companynumb": "reporter_company",
                "primarysource.qualification": "reporter_qualification",
                "primarysource.reportercountry": "reporter_country",
            }
        )
        .dropna(subset=[PRIMARY_KEY])
    )
    reporter = reporter.sort_values(by=[PRIMARY_KEY]).drop_duplicates(subset=[PRIMARY_KEY], keep="first")
    reporter = sort_cols(reporter)
    write_csv_gz(reporter, er_dir / "reporter.csv.gz")


def step_patient(data_dir: Path, er_dir: Path, workers: int, max_files: Optional[int]) -> None:
    dir_ = data_dir / "patient"
    files = sorted(dir_.glob("*.csv.gzip"))
    usecols = [
        PRIMARY_KEY,
        "patient.patientonsetage",
        "patient.patientonsetageunit",
        "master_age",
        "patient.patientsex",
        "patient.patientweight",
    ]
    dtype = {PRIMARY_KEY: "str"}
    df = concat_files(files, usecols=usecols, dtype=dtype, workers=workers, desc="patient", max_files=max_files)
    to_str(df, PRIMARY_KEY)

    out = (
        df.rename(
            columns={
                "patient.patientonsetage": "patient_onsetage",
                "patient.patientonsetageunit": "patient_onsetageunit",
                "master_age": "patient_custom_master_age",
                "patient.patientsex": "patient_sex",
                "patient.patientweight": "patient_weight",
            }
        )
        .dropna(subset=[PRIMARY_KEY])
        .sort_values(by=[PRIMARY_KEY])
        .drop_duplicates(subset=[PRIMARY_KEY], keep="first")
    )
    out = sort_cols(out)
    write_csv_gz(out, er_dir / "patient.csv.gz")


def step_patient_drug(data_dir: Path, er_dir: Path, workers: int, max_files: Optional[int]) -> None:
    # drug characteristics
    dir_drug = data_dir / "patient_drug"
    files_drug = sorted(dir_drug.glob("*.csv.gzip"))
    usecols_drug = [
        PRIMARY_KEY,
        "medicinalproduct",
        "drugcharacterization",
        "drugadministrationroute",
        "drugindication",
    ]
    dtype = {PRIMARY_KEY: "str"}
    df_drug = concat_files(
        files_drug, usecols=usecols_drug, dtype=dtype, workers=workers, desc="patient_drug", max_files=max_files
    )
    to_str(df_drug, PRIMARY_KEY)
    out_drugchar = (
        df_drug.rename(
            columns={
                "medicinalproduct": "medicinal_product",
                "drugcharacterization": "drug_characterization",
                "drugadministrationroute": "drug_administration",
                "drugindication": "drug_indication",
            }
        )
        .drop_duplicates()
        .dropna(subset=[PRIMARY_KEY])
    )
    out_drugchar = sort_cols(out_drugchar)
    write_csv_gz(out_drugchar, er_dir / "drugcharacteristics.csv.gz")

    # drugs.csv.gz from patient_drug_openfda_rxcui
    dir_rxcui = data_dir / "patient_drug_openfda_rxcui"
    files_rxcui = sorted(dir_rxcui.glob("*.csv.gzip"))
    usecols_rxcui = [PRIMARY_KEY, "value"]
    df_rxcui = concat_files(
        files_rxcui, usecols=usecols_rxcui, dtype={PRIMARY_KEY: "str"}, workers=workers, desc="rxcui", max_files=max_files
    )
    to_str(df_rxcui, PRIMARY_KEY)
    # Safe int conversion for value -> rxcui
    df_rxcui = df_rxcui.rename(columns={"value": "rxcui"}).dropna(subset=[PRIMARY_KEY, "rxcui"])
    # Coerce to numeric and drop non-numeric
    df_rxcui["rxcui"] = pd.to_numeric(df_rxcui["rxcui"], errors="coerce")
    df_rxcui = df_rxcui.dropna(subset=["rxcui"])  # keep matches original .astype(int) behavior
    df_rxcui["rxcui"] = df_rxcui["rxcui"].astype(int)
    out_drugs = df_rxcui.drop_duplicates().reset_index(drop=True)
    out_drugs = sort_cols(out_drugs)
    write_csv_gz(out_drugs, er_dir / "drugs.csv.gz")


def step_patient_reaction(data_dir: Path, er_dir: Path, workers: int, max_files: Optional[int]) -> None:
    dir_ = data_dir / "patient_reaction"
    files = sorted(dir_.glob("*.csv.gzip"))
    usecols = [PRIMARY_KEY, "reactionmeddrapt", "reactionoutcome"]
    df = concat_files(files, usecols=usecols, dtype={PRIMARY_KEY: "str"}, workers=workers, desc="reaction", max_files=max_files)
    to_str(df, PRIMARY_KEY)
    out = (
        df.rename(columns={"reactionmeddrapt": "reaction_meddrapt", "reactionoutcome": "reaction_outcome"})
        .dropna(subset=[PRIMARY_KEY])
        .drop_duplicates()
    )
    out = sort_cols(out)
    write_csv_gz(out, er_dir / "reactions.csv.gz")


def load_vocab(vocab_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    concept = pd.read_csv(
        vocab_dir / "CONCEPT.csv",
        sep="\t",
        dtype={"concept_id": "int", "concept_code": "object"},
        low_memory=False,
    )
    concept_relationship = pd.read_csv(
        vocab_dir / "CONCEPT_RELATIONSHIP.csv",
        sep="\t",
        dtype={"concept_id_1": "int", "concept_id_2": "int", "relationship_id": "object"},
        low_memory=False,
    )
    return concept, concept_relationship


def step_standard_drugs(er_dir: Path, data_dir: Path, concept: pd.DataFrame) -> None:
    drugs = pd.read_csv(er_dir / "drugs.csv.gz", compression="gzip", dtype={PRIMARY_KEY: "str"})
    # rxcui -> RxNorm concept_code (int)
    drugs["rxcui"] = drugs["rxcui"].astype(int)

    rx = concept.query('vocabulary_id=="RxNorm"').loc[
        :, ["concept_id", "concept_code", "concept_name", "concept_class_id"]
    ]
    # concept_code for RxNorm is numeric string -> cast to int for join
    rx = rx.dropna(subset=["concept_code"]).copy()
    # coerce to numeric quietly and drop NaNs
    rx["concept_code"] = pd.to_numeric(rx["concept_code"], errors="coerce")
    rx = rx.dropna(subset=["concept_code"]).copy()
    rx["concept_code"] = rx["concept_code"].astype(int)

    joined = (
        drugs.merge(
            rx.rename(columns={"concept_code": "RxNorm_concept_code"}),
            left_on="rxcui",
            right_on="RxNorm_concept_code",
            how="inner",
            validate="many_to_one",
        )
        .rename(
            columns={
                "concept_id": "RxNorm_concept_id",
                "concept_name": "RxNorm_concept_name",
                "concept_class_id": "RxNorm_concept_class_id",
            }
        )
        .drop_duplicates()
    )
    joined = sort_cols(joined)

    # Persist ids for later step (ingredients)
    ids = joined["RxNorm_concept_id"].dropna().astype(int).unique()
    with gzip.open(data_dir / "all_openFDA_rxnorm_concept_ids.pkl", "wb") as f:
        import pickle

        pickle.dump(ids, f)

    write_csv_gz(joined, er_dir / "standard_drugs.csv.gz")


def step_standard_reactions(er_dir: Path, data_dir: Path, concept: pd.DataFrame) -> None:
    reactions = pd.read_csv(er_dir / "reactions.csv.gz", compression="gzip", dtype={PRIMARY_KEY: "str"})
    med = concept.query('vocabulary_id=="MedDRA"').loc[
        :, ["concept_id", "concept_name", "concept_code", "concept_class_id"]
    ]

    # Title-case both sides like the original
    reactions["reaction_meddrapt"] = reactions["reaction_meddrapt"].astype(str).str.title()
    med = med.copy()
    med["concept_name"] = med["concept_name"].astype(str).str.title()

    joined = (
        reactions.merge(
            med.query('concept_class_id=="PT"').rename(
                columns={
                    "concept_id": "MedDRA_concept_id",
                    "concept_code": "MedDRA_concept_code",
                    "concept_name": "MedDRA_concept_name",
                    "concept_class_id": "MedDRA_concept_class_id",
                }
            ),
            left_on="reaction_meddrapt",
            right_on="MedDRA_concept_name",
            how="left",
        )
        .drop_duplicates()
    )

    joined_notnull = joined[joined["MedDRA_concept_id"].notna()].copy()
    joined_notnull["MedDRA_concept_id"] = joined_notnull["MedDRA_concept_id"].astype(int)
    joined_notnull = sort_cols(joined_notnull)

    # Persist ids (not used later, but mirror original intent)
    ids = joined_notnull["MedDRA_concept_id"].astype(int).unique()
    with gzip.open(data_dir / "all_openFDA_meddra_concept_ids.pkl", "wb") as f:
        import pickle

        pickle.dump(ids, f)

    write_csv_gz(joined_notnull, er_dir / "standard_reactions.csv.gz")


def step_standard_drugs_atc(
    er_dir: Path, concept: pd.DataFrame, concept_relationship: pd.DataFrame
) -> None:
    std = pd.read_csv(er_dir / "standard_drugs.csv.gz", compression="gzip", dtype={PRIMARY_KEY: "str"})
    std["RxNorm_concept_id"] = std["RxNorm_concept_id"].astype(int)

    rx = concept.query('vocabulary_id=="RxNorm"').loc[
        :, ["concept_id", "concept_code", "concept_name", "concept_class_id"]
    ].drop_duplicates()
    rx["concept_id"] = rx["concept_id"].astype(int)

    atc = concept.query('vocabulary_id=="ATC" & concept_class_id=="ATC 5th"').loc[
        :, ["concept_id", "concept_code", "concept_name", "concept_class_id"]
    ]
    atc["concept_id"] = atc["concept_id"].astype(int)

    rel = concept_relationship.loc[:, ["concept_id_1", "concept_id_2", "relationship_id"]].drop_duplicates()
    rel["concept_id_1"] = rel["concept_id_1"].astype(int)
    rel["concept_id_2"] = rel["concept_id_2"].astype(int)

    # Map RxNorm -> ATC
    r2a = (
        rel.merge(
            rx.rename(columns={
                "concept_id": "RxNorm_concept_id",
                "concept_code": "RxNorm_concept_code",
                "concept_name": "RxNorm_concept_name",
                "concept_class_id": "RxNorm_concept_class_id",
            }),
            left_on="concept_id_1",
            right_on="RxNorm_concept_id",
            how="inner",
        )
        .merge(
            atc.rename(
                columns={
                    "concept_id": "ATC_concept_id",
                    "concept_code": "ATC_concept_code",
                    "concept_name": "ATC_concept_name",
                    "concept_class_id": "ATC_concept_class_id",
                }
            ),
            left_on="concept_id_2",
            right_on="ATC_concept_id",
            how="inner",
        )
        .drop(columns=["concept_id_1", "concept_id_2"])
    )

    out = (
        std.loc[:, ["RxNorm_concept_id", PRIMARY_KEY]]
        .drop_duplicates()
        .merge(r2a, on="RxNorm_concept_id", how="left")
        .drop(columns=[
            "RxNorm_concept_code",
            "RxNorm_concept_name",
            "RxNorm_concept_class_id",
            "relationship_id",
        ])
        .dropna(subset=["ATC_concept_id"])
        .drop_duplicates()
    )
    out["ATC_concept_id"] = out["ATC_concept_id"].astype(int)
    out = sort_cols(out)
    write_csv_gz(out, er_dir / "standard_drugs_atc.csv.gz")


def step_standard_drugs_rxnorm_ingredients(
    er_dir: Path, data_dir: Path, concept: pd.DataFrame, concept_relationship: pd.DataFrame
) -> None:
    # Load ids kept earlier
    import pickle
    with gzip.open(data_dir / "all_openFDA_rxnorm_concept_ids.pkl", "rb") as f:
        rx_ids: np.ndarray = pickle.load(f)

    r = concept_relationship.loc[:, ["concept_id_1", "concept_id_2", "relationship_id"]].drop_duplicates().copy()
    r["concept_id_1"] = r["concept_id_1"].astype(int)
    r["concept_id_2"] = r["concept_id_2"].astype(int)

    c = (
        concept.query('vocabulary_id=="RxNorm" & standard_concept=="S"')[
            ["concept_id", "concept_code", "concept_class_id", "concept_name"]
        ]
        .drop_duplicates()
        .copy()
    )
    c["concept_id"] = c["concept_id"].astype(int)

    # First -> Second
    fs = (
        r[r["concept_id_1"].isin(rx_ids)]
        .merge(
            c.rename(
                columns={
                    "concept_id": "RxNorm_concept_id_1",
                    "concept_code": "RxNorm_concept_code_1",
                    "concept_class_id": "RxNorm_concept_class_id_1",
                    "concept_name": "RxNorm_concept_name_1",
                }
            ),
            left_on="concept_id_1",
            right_on="RxNorm_concept_id_1",
            how="inner",
        )
        .merge(
            c.rename(
                columns={
                    "concept_id": "RxNorm_concept_id_2",
                    "concept_code": "RxNorm_concept_code_2",
                    "concept_class_id": "RxNorm_concept_class_id_2",
                    "concept_name": "RxNorm_concept_name_2",
                }
            ),
            left_on="concept_id_2",
            right_on="RxNorm_concept_id_2",
            how="inner",
        )
        .rename(columns={"relationship_id": "relationship_id_12"})
    )
    fs = fs[fs["RxNorm_concept_id_1"] != fs["RxNorm_concept_id_2"]].drop_duplicates()

    # Second -> Third
    ids = fs["RxNorm_concept_id_2"].astype(int).unique()
    st = (
        r[r["concept_id_1"].isin(ids)]
        .merge(
            c.rename(
                columns={
                    "concept_id": "RxNorm_concept_id_2",
                    "concept_code": "RxNorm_concept_code_2",
                    "concept_class_id": "RxNorm_concept_class_id_2",
                    "concept_name": "RxNorm_concept_name_2",
                }
            ),
            left_on="concept_id_1",
            right_on="RxNorm_concept_id_2",
            how="inner",
        )
        .merge(
            c.rename(
                columns={
                    "concept_id": "RxNorm_concept_id_3",
                    "concept_code": "RxNorm_concept_code_3",
                    "concept_class_id": "RxNorm_concept_class_id_3",
                    "concept_name": "RxNorm_concept_name_3",
                }
            ),
            left_on="concept_id_2",
            right_on="RxNorm_concept_id_3",
            how="inner",
        )
        .rename(columns={"relationship_id": "relationship_id_23"})
    )
    st = st[st["RxNorm_concept_id_2"] != st["RxNorm_concept_id_3"]].drop_duplicates()

    # Third -> Fourth
    ids = st["RxNorm_concept_id_3"].astype(int).unique()
    tf = (
        r[r["concept_id_1"].isin(ids)]
        .merge(
            c.rename(
                columns={
                    "concept_id": "RxNorm_concept_id_3",
                    "concept_code": "RxNorm_concept_code_3",
                    "concept_class_id": "RxNorm_concept_class_id_3",
                    "concept_name": "RxNorm_concept_name_3",
                }
            ),
            left_on="concept_id_1",
            right_on="RxNorm_concept_id_3",
            how="inner",
        )
        .merge(
            c.rename(
                columns={
                    "concept_id": "RxNorm_concept_id_4",
                    "concept_code": "RxNorm_concept_code_4",
                    "concept_class_id": "RxNorm_concept_class_id_4",
                    "concept_name": "RxNorm_concept_name_4",
                }
            ),
            left_on="concept_id_2",
            right_on="RxNorm_concept_id_4",
            how="inner",
        )
        .rename(columns={"relationship_id": "relationship_id_34"})
    )
    tf = tf[tf["RxNorm_concept_id_3"] != tf["RxNorm_concept_id_4"]].drop_duplicates()

    # Fourth -> Fifth
    ids = tf["RxNorm_concept_id_4"].astype(int).unique()
    ff = (
        r[r["concept_id_1"].isin(ids)]
        .merge(
            c.rename(
                columns={
                    "concept_id": "RxNorm_concept_id_4",
                    "concept_code": "RxNorm_concept_code_4",
                    "concept_class_id": "RxNorm_concept_class_id_4",
                    "concept_name": "RxNorm_concept_name_4",
                }
            ),
            left_on="concept_id_1",
            right_on="RxNorm_concept_id_4",
            how="inner",
        )
        .merge(
            c.rename(
                columns={
                    "concept_id": "RxNorm_concept_id_5",
                    "concept_code": "RxNorm_concept_code_5",
                    "concept_class_id": "RxNorm_concept_class_id_5",
                    "concept_name": "RxNorm_concept_name_5",
                }
            ),
            left_on="concept_id_2",
            right_on="RxNorm_concept_id_5",
            how="inner",
        )
        .rename(columns={"relationship_id": "relationship_id_45"})
    )
    ff = ff[ff["RxNorm_concept_id_4"] != ff["RxNorm_concept_id_5"]].drop_duplicates()

    # Fifth -> Sixth
    ids = ff["RxNorm_concept_id_5"].astype(int).unique()
    fsx = (
        r[r["concept_id_1"].isin(ids)]
        .merge(
            c.rename(
                columns={
                    "concept_id": "RxNorm_concept_id_5",
                    "concept_code": "RxNorm_concept_code_5",
                    "concept_class_id": "RxNorm_concept_class_id_5",
                    "concept_name": "RxNorm_concept_name_5",
                }
            ),
            left_on="concept_id_1",
            right_on="RxNorm_concept_id_5",
            how="inner",
        )
        .merge(
            c.rename(
                columns={
                    "concept_id": "RxNorm_concept_id_6",
                    "concept_code": "RxNorm_concept_code_6",
                    "concept_class_id": "RxNorm_concept_class_id_6",
                    "concept_name": "RxNorm_concept_name_6",
                }
            ),
            left_on="concept_id_2",
            right_on="RxNorm_concept_id_6",
            how="inner",
        )
        .rename(columns={"relationship_id": "relationship_id_56"})
    )
    fsx = fsx[fsx["RxNorm_concept_id_5"] != fsx["RxNorm_concept_id_6"]].drop_duplicates()

    # Build ingredient mappings like original (via 1-2-3 and 1-2-3-4 paths)
    rx123 = (
        fs.merge(st, on=["RxNorm_concept_id_2", "RxNorm_concept_code_2", "RxNorm_concept_name_2", "RxNorm_concept_class_id_2"])  # type: ignore  # noqa
        .query('RxNorm_concept_class_id_3=="Ingredient" and RxNorm_concept_class_id_1!=RxNorm_concept_class_id_3')
        .drop_duplicates()
    )
    rx123_to_add = (
        rx123.loc[
            :,
            [
                "RxNorm_concept_id_1",
                "RxNorm_concept_code_1",
                "RxNorm_concept_name_1",
                "RxNorm_concept_class_id_1",
                "RxNorm_concept_id_3",
                "RxNorm_concept_code_3",
                "RxNorm_concept_name_3",
                "RxNorm_concept_class_id_3",
            ],
        ]
        .rename(
            columns={
                "RxNorm_concept_id_3": "RxNorm_concept_id_2",
                "RxNorm_concept_code_3": "RxNorm_concept_code_2",
                "RxNorm_concept_name_3": "RxNorm_concept_name_2",
                "RxNorm_concept_class_id_3": "RxNorm_concept_class_id_2",
            }
        )
        .drop_duplicates()
    )

    rx1234 = (
        fs.merge(st, on=["RxNorm_concept_id_2", "RxNorm_concept_code_2", "RxNorm_concept_name_2", "RxNorm_concept_class_id_2"])  # type: ignore  # noqa
        .query('RxNorm_concept_class_id_3!="Ingredient" and RxNorm_concept_class_id_1!=RxNorm_concept_class_id_3')
        .merge(
            tf,
            on=[
                "RxNorm_concept_id_3",
                "RxNorm_concept_code_3",
                "RxNorm_concept_name_3",
                "RxNorm_concept_class_id_3",
            ],
        )
        .query('RxNorm_concept_class_id_4=="Ingredient"')
        .drop_duplicates()
    )

    rx_all = pd.concat([rx123_to_add, rx1234], ignore_index=True, sort=False).drop_duplicates()

    # Attach to standard_drugs to materialize per-report ingredient rows
    std = pd.read_csv(er_dir / "standard_drugs.csv.gz", compression="gzip", dtype={PRIMARY_KEY: "str"})
    std["RxNorm_concept_id"] = std["RxNorm_concept_id"].astype(int)
    std_ing = (
        std.loc[:, ["RxNorm_concept_id", PRIMARY_KEY]]
        .drop_duplicates()
        .merge(
            rx_all.rename(
                columns={
                    "RxNorm_concept_id_1": "RxNorm_concept_id",
                    "RxNorm_concept_id_2": "RxNorm_concept_id_ing",
                    "RxNorm_concept_code_2": "RxNorm_concept_code",
                    "RxNorm_concept_name_2": "RxNorm_concept_name",
                    "RxNorm_concept_class_id_2": "RxNorm_concept_class_id",
                }
            ),
            on=["RxNorm_concept_id"],
            how="left",
        )
        .dropna(subset=["RxNorm_concept_id_ing"])
        .drop(columns=["RxNorm_concept_id_ing"])  # align with original columns
        .drop_duplicates()
    )
    std_ing = sort_cols(std_ing)
    write_csv_gz(std_ing, er_dir / "standard_drugs_rxnorm_ingredients.csv.gz")


def step_standard_reactions_meddra_relationships(
    er_dir: Path, concept: pd.DataFrame, concept_relationship: pd.DataFrame
) -> None:
    std_rx = pd.read_csv(er_dir / "standard_reactions.csv.gz", compression="gzip", dtype={PRIMARY_KEY: "str"})
    med = concept.query('vocabulary_id=="MedDRA"').copy()
    med["concept_id"] = med["concept_id"].astype(int)

    r = concept_relationship.loc[:, ["concept_id_1", "concept_id_2", "relationship_id"]].drop_duplicates().copy()
    r["concept_id_1"] = r["concept_id_1"].astype(int)
    r["concept_id_2"] = r["concept_id_2"].astype(int)

    all_meddra = (
        r.merge(
            med.loc[:, ["concept_id", "concept_code", "concept_name", "concept_class_id"]].rename(
                columns={
                    "concept_id": "MedDRA_concept_id_1",
                    "concept_code": "MedDRA_concept_code_1",
                    "concept_name": "MedDRA_concept_name_1",
                    "concept_class_id": "MedDRA_concept_class_id_1",
                }
            ),
            left_on="concept_id_1",
            right_on="MedDRA_concept_id_1",
            how="inner",
        )
        .merge(
            med.loc[:, ["concept_id", "concept_code", "concept_name", "concept_class_id"]].rename(
                columns={
                    "concept_id": "MedDRA_concept_id_2",
                    "concept_code": "MedDRA_concept_code_2",
                    "concept_name": "MedDRA_concept_name_2",
                    "concept_class_id": "MedDRA_concept_class_id_2",
                }
            ),
            left_on="concept_id_2",
            right_on="MedDRA_concept_id_2",
            how="inner",
        )
        .drop(columns=["concept_id_1", "concept_id_2"])
    )

    # PT -> HLT
    first_rel = all_meddra.query('MedDRA_concept_class_id_2=="HLT"').copy()
    first_rel = first_rel[first_rel["MedDRA_concept_id_1"] != first_rel["MedDRA_concept_id_2"]]

    # HLT -> HLGT
    second_rel = (
        all_meddra.merge(
            first_rel[["MedDRA_concept_id_2"]].drop_duplicates().rename(columns={"MedDRA_concept_id_2": "MedDRA_concept_id_1"}),
            on="MedDRA_concept_id_1",
            how="inner",
        )
        .rename(
            columns={
                "MedDRA_concept_id_1": "MedDRA_concept_id_2",
                "MedDRA_concept_code_1": "MedDRA_concept_code_2",
                "MedDRA_concept_name_1": "MedDRA_concept_name_2",
                "MedDRA_concept_class_id_1": "MedDRA_concept_class_id_2",
                "MedDRA_concept_id_2": "MedDRA_concept_id_3",
                "MedDRA_concept_code_2": "MedDRA_concept_code_3",
                "MedDRA_concept_name_2": "MedDRA_concept_name_3",
                "MedDRA_concept_class_id_2": "MedDRA_concept_class_id_3",
                "relationship_id": "relationship_id_23",
            }
        )
    )
    second_rel = second_rel[second_rel["MedDRA_concept_class_id_3"] == "HLGT"]
    second_rel = second_rel[second_rel["MedDRA_concept_id_2"] != second_rel["MedDRA_concept_id_3"]]

    # HLGT -> SOC
    third_rel = (
        all_meddra.merge(
            second_rel[["MedDRA_concept_id_3"]].drop_duplicates().rename(columns={"MedDRA_concept_id_3": "MedDRA_concept_id_1"}),
            on="MedDRA_concept_id_1",
            how="inner",
        )
        .rename(
            columns={
                "MedDRA_concept_id_1": "MedDRA_concept_id_3",
                "MedDRA_concept_code_1": "MedDRA_concept_code_3",
                "MedDRA_concept_name_1": "MedDRA_concept_name_3",
                "MedDRA_concept_class_id_1": "MedDRA_concept_class_id_3",
                "MedDRA_concept_id_2": "MedDRA_concept_id_4",
                "MedDRA_concept_code_2": "MedDRA_concept_code_4",
                "MedDRA_concept_name_2": "MedDRA_concept_name_4",
                "MedDRA_concept_class_id_2": "MedDRA_concept_class_id_4",
                "relationship_id": "relationship_id_34",
            }
        )
    )
    third_rel = third_rel[third_rel["MedDRA_concept_class_id_4"] == "SOC"]
    third_rel = third_rel[third_rel["MedDRA_concept_id_3"] != third_rel["MedDRA_concept_id_4"]]

    # Stitch with standard_reactions (PT at level 1)
    std_rx_pt = std_rx.loc[:, [PRIMARY_KEY, "MedDRA_concept_id"]].drop_duplicates()
    std_rx_pt["MedDRA_concept_id"] = std_rx_pt["MedDRA_concept_id"].astype(int)

    pt_to_hlt = (
        std_rx_pt.merge(
            first_rel.rename(columns={"MedDRA_concept_id_1": "MedDRA_concept_id"}),
            on="MedDRA_concept_id",
            how="inner",
        )
        .rename(
            columns={
                "MedDRA_concept_id_2": "MedDRA_concept_id",
                "MedDRA_concept_code_2": "MedDRA_concept_code",
                "MedDRA_concept_name_2": "MedDRA_concept_name",
                "MedDRA_concept_class_id_2": "MedDRA_concept_class_id",
            }
        )
        .drop_duplicates()
    )
    pt_to_hlgt = (
        std_rx_pt.merge(
            second_rel.rename(columns={"MedDRA_concept_id_2": "MedDRA_concept_id"}),
            on="MedDRA_concept_id",
            how="inner",
        )
        .rename(
            columns={
                "MedDRA_concept_id_3": "MedDRA_concept_id",
                "MedDRA_concept_code_3": "MedDRA_concept_code",
                "MedDRA_concept_name_3": "MedDRA_concept_name",
                "MedDRA_concept_class_id_3": "MedDRA_concept_class_id",
            }
        )
        .drop_duplicates()
    )
    pt_to_soc = (
        std_rx_pt.merge(
            third_rel.rename(columns={"MedDRA_concept_id_3": "MedDRA_concept_id"}),
            on="MedDRA_concept_id",
            how="inner",
        )
        .rename(
            columns={
                "MedDRA_concept_id_4": "MedDRA_concept_id",
                "MedDRA_concept_code_4": "MedDRA_concept_code",
                "MedDRA_concept_name_4": "MedDRA_concept_name",
                "MedDRA_concept_class_id_4": "MedDRA_concept_class_id",
            }
        )
        .drop_duplicates()
    )

    # Combined relationships table akin to original (PT, HLT, HLGT, SOC per row)
    med_rel = (
        std_rx_pt.merge(first_rel, left_on="MedDRA_concept_id", right_on="MedDRA_concept_id_1", how="left")
        .merge(second_rel, on="MedDRA_concept_id_2", how="left")
        .merge(third_rel, left_on="MedDRA_concept_id_3", right_on="MedDRA_concept_id_3", how="left")
        .drop_duplicates()
    )

    # Write outputs
    write_csv_gz(sort_cols(med_rel), er_dir / "standard_reactions_meddra_relationships.csv.gz")
    write_csv_gz(sort_cols(pt_to_hlt), er_dir / "standard_reactions_meddra_hlt.csv.gz")
    write_csv_gz(sort_cols(pt_to_hlgt), er_dir / "standard_reactions_meddra_hlgt.csv.gz")
    write_csv_gz(sort_cols(pt_to_soc), er_dir / "standard_reactions_meddra_soc.csv.gz")


def step_standard_reactions_snomed(
    er_dir: Path, concept: pd.DataFrame, concept_relationship: pd.DataFrame
) -> None:
    std_rx = pd.read_csv(er_dir / "standard_reactions.csv.gz", compression="gzip", dtype={PRIMARY_KEY: "str"})
    std_rx["MedDRA_concept_id"] = std_rx["MedDRA_concept_id"].astype(int)

    # MedDRA -> SNOMED equivalence
    m2s = (
        concept_relationship.query('relationship_id=="MedDRA - SNOMED eq"')
        .loc[:, ["concept_id_1", "concept_id_2"]]
        .drop_duplicates()
        .rename(columns={"concept_id_1": "MedDRA_concept_id", "concept_id_2": "SNOMED_concept_id"})
    )

    snomed = concept.query('vocabulary_id=="SNOMED"').loc[
        :, ["concept_id", "concept_code", "concept_class_id", "concept_name"]
    ]
    snomed = snomed.rename(
        columns={
            "concept_id": "SNOMED_concept_id",
            "concept_code": "SNOMED_concept_code",
            "concept_class_id": "SNOMED_concept_class_id",
            "concept_name": "SNOMED_concept_name",
        }
    )

    out = (
        std_rx.merge(m2s, on="MedDRA_concept_id", how="inner")
        .merge(snomed, on="SNOMED_concept_id", how="inner")
        .drop_duplicates()
    )
    write_csv_gz(sort_cols(out), er_dir / "standard_reactions_snomed.csv.gz")


def main():
    ap = argparse.ArgumentParser(description="Build ER tables (optimized)")
    ap.add_argument(
        "--data-dir",
        default="data/openFDA_drug_event",
        help="Input data directory containing openFDA CSVs",
    )
    ap.add_argument(
        "--vocab-dir",
        default="vocab/vocabulary_SNOMED_MEDDRA_RxNorm_ATC",
        help="Vocabulary directory containing CONCEPT*.tsv files",
    )
    ap.add_argument(
        "--er-dir",
        default=None,
        help="Output directory for ER tables (defaults to <data-dir>/er_tables)",
    )
    ap.add_argument("--workers", type=int, default=4, help="Parallel file read workers")
    ap.add_argument("--steps", type=str, default="",
                    help="Comma-separated steps to run: report,patient,patient_drug,reactions,standard_drugs,standard_reactions,standard_drugs_atc,standard_drugs_rxnorm_ingredients,standard_reactions_snomed. Default = all")
    ap.add_argument("--max-files", type=int, default=None, help="Limit number of files per input folder (for testing)")
    ap.add_argument("-v", "--verbose", action="count", default=0)
    args = ap.parse_args()

    setup_logging(args.verbose)

    data_dir = Path(args.data_dir)
    er_dir = Path(args.er_dir) if args.er_dir else data_dir / "er_tables"
    vocab_dir = Path(args.vocab_dir)

    ensure_dir(er_dir)

    requested = [s.strip() for s in (args.steps.split(",") if args.steps else []) if s.strip()]

    def want(step: str) -> bool:
        return (not requested) or (step in requested)

    if want("report"):
        logging.info("Step 1/8: report tables")
        step_report(data_dir, er_dir, workers=args.workers, max_files=args.max_files)

    if want("patient"):
        logging.info("Step 2/8: patient table")
        step_patient(data_dir, er_dir, workers=args.workers, max_files=args.max_files)

    if want("patient_drug"):
        logging.info("Step 3/8: drug characteristics + drugs")
        step_patient_drug(data_dir, er_dir, workers=args.workers, max_files=args.max_files)

    if want("reactions"):
        logging.info("Step 4/8: reactions")
        step_patient_reaction(data_dir, er_dir, workers=args.workers, max_files=args.max_files)

    # Vocab required from here
    if any(
        want(s)
        for s in [
            "standard_drugs",
            "standard_reactions",
            "standard_drugs_atc",
            "standard_drugs_rxnorm_ingredients",
            "standard_reactions_snomed",
        ]
    ):
        logging.info("Step 5/8: load vocab")
        concept, concept_relationship = load_vocab(vocab_dir)
    else:
        concept = concept_relationship = None  # type: ignore

    if want("standard_drugs"):
        logging.info("Step 6/8: standard drugs (RxNorm)")
        step_standard_drugs(er_dir, data_dir, concept)  # type: ignore

    if want("standard_reactions"):
        logging.info("Step 7/8: standard reactions (MedDRA)")
        step_standard_reactions(er_dir, data_dir, concept)  # type: ignore

    if want("standard_drugs_atc"):
        logging.info("Step 8a/8: standard drugs -> ATC")
        step_standard_drugs_atc(er_dir, concept, concept_relationship)  # type: ignore

    if want("standard_drugs_rxnorm_ingredients"):
        logging.info("Step 8b/8: standard drugs -> RxNorm ingredients")
        step_standard_drugs_rxnorm_ingredients(er_dir, data_dir, concept, concept_relationship)  # type: ignore

    if want("standard_reactions_snomed"):
        logging.info("Step 8c/8: standard reactions -> SNOMED")
        step_standard_reactions_snomed(er_dir, concept, concept_relationship)  # type: ignore

    logging.info("Done.")


if __name__ == "__main__":
    main()
