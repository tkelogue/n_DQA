# coding: latin-1
from __future__ import annotations
import os
import json
import polars as pl
import polars.selectors as cs
import pandas as pd
import openpyxl as xl

from os import path
from datetime import datetime

# Get schema from db
#   pass
# Else import it from schema file (from exact location)
from schema import schema as sh
from schema import schema_Id
from schema import sheet_name
from py_utils import polarsType, pandasType, df2x_dict


def read_File_pd(file: str, sheet="data") -> dict:  mande
    """
    Data  Quality analysis implementation with Pandas 2.0 or latest
    args:
        file: str, le chemin vers le fichier de données à analyser
        sheet: str, indiquant le nom du sheet/de la feuille,
        requis seulement si le fichier est au format excel.
        Valeur par défaut 'data'.
     return:
        un dictionnaire de métrique:
            {





            }
    """
    print(10000, datetime.now().timestamp())
    start = datetime.now().timestamp()
    ext = file.split(".")[-1]
    if ext not in ["csv", "xls", "xlsx", "json"]:
        print(f"Support seulement pour fichier au format .csv, .xls, .xlsx ou .json")
        return

    elif ext == "csv":
        pdfp = pd.read_csv(file, encoding="utf-8", parse_dates=True)

    # read xls file with polars
    elif ext == "xls" or ext == "xlsx":
        pdfp = pd.read_excel(file, sheet_name=sheet, parse_dates=True, index_col=None)

    # read json file with polars
    elif ext == "json":
        pdfp = pd.read_json(file, index_col=None)

    # normalize field names
    pdfp = pdfp.rename(columns=str.lower)

    # extract volumetric statistics
    cols = pdfp.columns.array
    nCols = pdfp.shape[1]
    nRows = pdfp.shape[0]

    # compare expected field names
    exp_fields = [*sh.keys()]
    is_detected = [f.lower() in cols for f in exp_fields]
    missed_fields = [f.lower() for f in exp_fields if f.lower() not in cols]
    not_expected_fields = [f.lower() for f in cols if f.lower() not in exp_fields]

    # try to cast each field detected based upon expected schema type
    exp_types = {col: sh.get(col).get("type") for col in exp_fields}
    """
    for el in exp_types:
        if el in cols:
            _type = pandasType(exp_types.get(el))
            if _type.startswith("int") or _type.startswith("fl"):
                pdfp[el] = pd.to_numeric(el, downcast=_type, errors="coerce")
            elif _type.startswith("boo"):
                pdfp[el] = pdfp[el].apply(boolean)
            elif _type.startswith("decimal"):
                pdfp[el] = pdfp[el].apply(float64)
            elif _type.startswith("date"):
                pdfp[el] = pdfp[el].apply(DateTime())
    print(pdfp)
    """
    # compare expected field types
    schema = pdfp.dtypes.apply(lambda t: "string" if t == "object" else t)
    obs_field_types = {
        col: str("string" if pdfp[col].dtypes == "object" else pdfp[col].dtypes).lower()
        for col in pdfp.columns
    }

    # print(obs_field_types)
    matched_ftypes = [
        el for el in exp_types if obs_field_types.get(el) == exp_types.get(el)
    ]
    not_matched_ftypes = [
        {el: [exp_types.get(el), obs_field_types.get(el)]}
        for el in exp_types
        if obs_field_types.get(el) != exp_types.get(el)
    ]

    # Profiler
    descriptive = pdfp.describe(percentiles=[0.1, 0.25, 0.5, 0.75, 0.8, 0.95])

    pdfpt = pd.DataFrame()
    for col in cols:
        pdfpt[col] = pdfp[col].apply(
            lambda d: str(type(d)).split("'")[1] if d != "null" else "'undefined"
        )

    typeStats = [pdfpt[col].value_counts().item for col in cols]

    print(typeStats)


##************************************************************##
#                           WITH POLARS                        #
##************************************************************##
def read_File_pl(file: str, sheet: str = None) -> dict:
    start = datetime.now().timestamp()
    ext = file.split(".")[-1]
    if ext not in ["csv", "xls", "xlsx", "json"]:
        msg = "Support seulement pour fichier au format .csv, .xls, .xlsx ou .json"
        return {"erreur": msg}

    # read csv file with polars
    if ext == "csv":
        dfp = pl.scan_csv(
            source=file,
            separator=sep,
            encoding="utf8",
            try_parse_dates=True,
            quote_char='"',
            infer_schema_length=0,
            has_header=True,
            with_column_names=lambda cols: [col.lower() for col in cols],
        )

    # read excel file with polars
    elif ext == "xlsx" or ext == "xls":
        pxl = xl.load_workbook(file)
        sheets = pxl.sheetnames
        if sheet is None or sheet not in sheets:
            if "Sheet1" in sheets:
                sheet = "Sheet1"
            elif "Feuille1" in sheets:
                sheet = "Feuille"
            else:
                sheet = sheets[0]
        else:
            pass
        dfp = pl.read_excel(
            source=file,
            sheet_name=sheet,
            read_csv_options={
                "infer_schema_length": 0,
                "quote_char": None,
                "try_parse_dates": True,
                "encoding": "utf8",
                "has_header": True,
            },
        )

        dfp.columns = [col.lower() for col in dfp.columns]

    # read json file with polars
    elif ext == "json":
        dfp = pl.scan_ndjson(source=file, infer_schema_length=0)
        dfp.rename(lambda col: col.lower())
        dfp.columns = [col.lower() for col in dfp.columns]

    # extract volumetric details
    nCols = dfp.width
    nRows = (
        dfp.select(pl.len()).collect().row(0)[0]
        if ext not in ["xls", "xlsx"]
        else dfp.shape[0]
    )

    # compare expected fields vs obs fields names
    cols = [col.lower() for col in dfp.columns]
    exp_fields = [col.lower() for col in [*sh.keys()]]
    missed_fields = [col for col in exp_fields if col not in cols]
    new_fields = [col for col in cols if col not in exp_fields]

    if len(missed_fields) == len(exp_fields):
        msg = "Aucun champ du schéma indiqué est détecté"
        return {"erreur": msg}

    # required fields list
    required_fields = [
        col
        for col in [*sh.keys()]
        if str(sh[col].get("requis")).lower().title() == "True"
    ]
    required_absent = [col for col in required_fields if col.lower() not in cols]
    if len(required_absent) > 0:
        msg = "Un ou plusieurs colonnes obligatoires sont manquantes"
        return {"erreur": msg, "msg": required_absent}

    # try to cast each field detected based upon expected schema type
    exp_types = {col: sh.get(col).get("type") for col in [*sh.keys()]}
    for k in exp_types:
        if k in cols:
            dfp = dfp.with_columns(
                pl.col(k).cast(polarsType(exp_types.get(k), 0), strict=False)
            )

    # comparer expected vs detected field types -> deux listes de colonnes ok et not ok
    schema = dfp.schema
    obs_field_types = {col: str(t).lower() for col, t in schema.items()}
    matched_ftypes = [
        k
        for k in exp_types
        if obs_field_types.get(k) == polarsType(exp_types.get(k), 1)
    ]
    not_matched_ftypes = {}
    for k in exp_types:
        if obs_field_types.get(k) != polarsType(exp_types.get(k), 1):
            not_matched_ftypes[k] = [exp_types.get(k), obs_field_types.get(k)]

    # calculer la distribution des valeurs selon leur type pour chaque colonne
    values_type = {}
    dfpt = dfp.select(
        pl.all().map_elements(
            lambda d: str(type(d)).split("'")[1] if d is not None else "undefined"
        )
    )
    for col in cols:
        if ext in ["xls", "xlsx"]:
            values_type[col] = df2x_dict(dfpt.group_by(col).len())
        else:
            values_type[col] = df2x_dict(dfpt.group_by(col).len().collect())
    dfpt = None

    # calculer le nb de valeurs manquantes dans chaque colonne
    missing_data = {
        col: dfp.select(pl.col(col).null_count())[0, 0]
        if ext in ["xls", "xlsx"]
        else dfp.select(pl.col(col).null_count()).collect()[0, 0]
        for col in cols
    }

    # détecter lignes complètes
    # dfp.filter(pl.)

    # pattern detection
    def check_patterns(col: str):
        pattern = sh[col]["pattern"]
        fails = []
        if pattern is None:
            return fails
        else:
            sdf = (
                dfp[col]
                if ext in ["xls", "xlsx"]
                else dfp.select(pl.col(col)).collect().to_series()
            )
            for item in sdf:
                if item is None:
                    pass
                else:
                    if pattern(item) == False:
                        fails.append(item)
            return fails

    pattern_fields = {}
    for col in cols:
        if "pattern" in sh[col].keys():
            pattern_fields[col] = check_patterns(col)

    # Min-Max check
    def check_MinMax(col: str):
        _min = sh[col]["minimum"]
        _max = sh[col]["maximum"]
        fails = []
        if _min is None and _max is None:
            return fails
        else:
            col_type: str = polarsType(exp_types.get(col), 1)
            if (
                col_type.startswith("int")
                or col_type.startswith("dec")
                or col_type.startswith("float")
            ):
                sdf = (
                    dfp[col]
                    if ext in ["xls", "xlsx"]
                    else dfp.select(pl.col(col)).collect().to_series()
                )
                for item in sdf:
                    if str(type(item)).split("'")[1].startswith("None"):
                        continue
                    elif str(type(item)).split("'")[1].startswith("int"):
                        pass
                    else:
                        item = str(item).replace(",", ".")

                    if len(fails) > 9:
                        break  # 10 failed check max
                    elif item is None:
                        continue
                    else:
                        if _min is not None and _max is not None:
                            if float(item) < _min or float(item) > _max:
                                fails.append(item)
                        elif _min is not None and _max is None:
                            if float(item) < _min:
                                fails.append(item)
                        elif _min is None and _max is not None:
                            if float(item) > _max:
                                fails.append(item)
                        else:
                            continue
                return fails
            elif col_type.startswith("str"):
                sdf = (
                    dfp[col]
                    if ext in ["xls", "xlsx"]
                    else dfp.select(pl.col(col)).collect().to_series()
                )
                for item in sdf:
                    if len(fails) > 9:
                        break  # 10 failed check max
                    if item is None:
                        continue
                    else:
                        if _min is not None and _max is not None:
                            if len(item) < _min or len(item) > _max:
                                fails.append(item)
                        elif _min is not None and _max is None:
                            if len(item) < _min:
                                fails.append(item)
                        elif _min is None and _max is not None:
                            if len(item) > _max:
                                fails.append(item)
                        else:
                            continue
                return fails

    minMax_fields = {}
    for col in cols:
        if "minimum" in sh[col].keys() or "maximum" in sh[col].keys():
            minMax_fields[col] = check_MinMax(col)

    # détecter répétition de ligne
    row_dup = (
        dfp.group_by(dfp.columns)
        .agg(pl.len().alias("count"))
        .filter(pl.col("count") > 1)
    )

    # calculer la distribution de fréquence pour chaque colonne
    values_freq = {}
    for col in cols:
        if obs_field_types.get(col).startswith("date"):
            dfp = dfp.with_columns(pl.col(col).cast(pl.Utf8, strict=False))

        if ext in ["xls", "xlsx"]:
            values_freq[str(col)] = dfp.group_by(col).len().to_dicts()
        else:
            values_freq[str(col)] = dfp.group_by(col).len().collect().to_dicts()

    # détecter non unicité des valeurs dans les colonnes selon le schéma (si unique == True)
    unique_fields = [
        col for col in exp_fields if str(sh[col].get("unique")).title() == "True"
    ]
    values_dup = {}
    for col in unique_fields:
        if col in cols:
            values_dup[col] = {
                str(k[col]): k["len"] for k in values_freq[col] if k["len"] > 1
            }
        else:
            pass

    # distribution description
    descriptive = (
        dfp.select(cs.numeric()).describe(
            percentiles=[0.1, 0.25, 0.5, 0.75, 0.8, 0.95],
            interpolation="linear",
        )
        if dfp.select(cs.numeric()).width
        else None
    )

    # DQA calculation compilation
    respond: dict = {
        "DQA_fonction": "Polars Version",
        "exe_time": {
            "debut": datetime.fromtimestamp(start).strftime("%Y-%m-%d %H:%M:%S"),
            "fin": datetime.fromtimestamp(datetime.now().timestamp()).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        },
        "fichier": file,
        "Sheet_Name": sheet if ext in ["xls", "xlsx"] else "N/A",
        "extension":ext,
        "schema_id": schema_Id,
        "esp_colonnes": exp_fields,
        "colonnes_obligatoires": required_fields,
        "obs_colonnes": cols,
        "colonnes_absentes": missed_fields,
        "colonnes_nouvelles": new_fields,
        "volumetrie": [nCols, nRows],
        "valeurs_manquantes": missing_data,
        "exp_types_col": exp_types,
        "obs_types_col": obs_field_types,
        "correct_type": matched_ftypes,
        "incorrect_type": not_matched_ftypes,
        "obs_types_valeur": values_type,
        "pattern_test": pattern_fields,
        "etendue_test": minMax_fields,
        "freq_dist": values_freq,
        "non_unique_valeur": values_dup,
        "repetition_ligne": row_dup.to_dicts()
        if row_dup is not None and ext in ["xls", "xlsx"]
        else row_dup.collect().to_dicts()
        if row_dup is not None
        else None,
        "descriptive_dist": descriptive.to_dicts() if descriptive is not None else None,
    }
    print(respond)
    return respond

