from __future__ import annotations
import polars as pl
from os import path

# Get schema from db
#   pass
# Else import it from schema file (from exact location)
from schema import schema as sh
from py_utils import polarsType


def read_File(file):
    ext = file.split(".")[-1]
    if ext not in ["csv", "xls", "xlsx", "json"]:
        print(f"Support seulement pour fichier au format .csv, .xls, .xlsx ou .json")
        return

    # read csv file with polars
    if ext == "csv":
        dfp = pl.scan_csv(
            source=file,
            separator=sep,
            encoding="utf8",
            try_parse_dates=True,
            quote_char=None,
            infer_schema_length=0,
            has_header=True,
            with_column_names=lambda cols: [col.lower() for col in cols],
        )

    # read excel file with polars
    elif ext == "xlsx" or ext == "xls":
        dfp = pl.read_excel(source=file)
        # sheet_name=sheet,
        """read_csv_options={
                "infer_schema_length": 0,
                "quote_char": None,
                "try_parse_dates": True,
                "encoding": "utf8",
                "has_header": True,
            },
        """

        dfp.columns = [col.lower() for col in dfp.columns]

    # read json file with polars
    elif ext == "json":
        dfp = pl.scan_ndjson(source=file, infer_schema_length=0)
        dfp.rename(lambda col: col.lower())
        dfp.columns = [col.lower() for col in dfp.columns]

    print(dfp) if ext in ["xls", "xlsx"] else print(dfp.collect())

    # extract statistics
    cols = dfp.columns
    nCols = dfp.width
    nRows = (
        dfp.select(pl.len()).collect().row(0)[0]
        if ext not in ["xls", "xlsx"]
        else dfp.shape[0]
    )

    """
    print(dfp.head(5))
    print(cols)
    print(schema)
    print(dtypes)
    print(nCols)
    print(nRows)
    """
    # compare expected field names
    exp_fields = [*sh.keys()]
    is_detected = [f.lower() in cols for f in exp_fields]
    missed_fields = [f.lower() for f in exp_fields if f.lower() not in cols]
    not_expected_fields = [f.lower() for f in cols if f.lower() not in exp_fields]

    """
    print("exp fields: ->", exp_fields)
    print("field is detected: -> ", is_detected)
    print("missed fields are: ->", missed_fields)
    print("not expected fields are: ->", not_expected_fields)
    """

    # try to cast each field detected based upon expected schema type
    exp_types = {col: sh.get(col).get("type") for col in exp_fields}
    for el in exp_types:
        if el in cols:
            dfp = dfp.with_columns(
                pl.col(el).cast(polarsType(exp_types.get(el)), strict=False)
            )

    # compare expected field types
    schema = dfp.schema
    obs_field_types = {col: str(t).lower() for col, t in schema.items()}
    matched_ftypes = [
        el for el in exp_types if obs_field_types.get(el) == exp_types.get(el)
    ]
    not_matched_ftypes = [
        {el: [exp_types.get(el), obs_field_types.get(el)]}
        for el in exp_types
        if obs_field_types.get(el) != exp_types.get(el)
    ]

    descriptive = dfp.describe(
        percentiles=[0.1, 0.25, 0.5, 0.75, 0.8, 0.95],
        interpolation="linear",
    )

    print(descriptive)

    # get type of each value for every detected and expected field col
    dfpt = None
    for col in cols:
        dfpt = dfp.with_columns(
            pl.col(col)
            .map_elements(lambda d: str(type(d)).split("'")[1])
            .alias("type_" + col)
        )

        print(dfpt.group_by("type_" + col).len()) if ext in ["xls", "xlsx"] else print(
            dfpt.group_by("type_" + col).len().collect()
        )

    # print(dfp)  # if ext in ["xls" "xlsx"] else print(dfp.collect())
    """
    print(exp_types)
    print(obs_field_types)
    print(matched_ftypes)
    print(not_matched_ftypes)
    """
    # print(dfp.null_count().collect())
    # print(dfp.random(10000).describe())
    # print(len(dfp.schema.keys))


# file to analyze
files = [
    "C:/Users/KT/source/csv xls xlsx analyzer/HiH Agricultural Typologies Dataset - Haiti.xlsx",
    "C:/Users/KT/Downloads/HiH Agricultural Typologies - Poverty Dataset - Haiti.csv",
    "C:/Users/KT/Documents/Scott2016.xlsx",
    "C:/Users/KT/Documents/quartiers2010.csv",
    "C:/Users/KT/Downloads/Urban population 1990-2010.xlsx",
    "C:/Users/KT/Downloads/20231120gleif.csv",
    "C:/Users/KT/Downloads/codeperi.xlsx",
    "C:/Users/KT/Downloads/Scott2016_Compilation _Analyse2016_02032017.xls",
]
f = 1
file = files[f]

sep = ","
sheet = "Sheet1"
path_test = path.isfile(file)
print(path_test)
if path_test is True:
    read_File(file)
