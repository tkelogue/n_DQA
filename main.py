# coding: latin-1
from __future__ import annotations
import os
import json

from os import path
from datetime import datetime
from pl_analyzer import read_File_pl as dqa_pl
from pl_analyzer import read_File_pd as dqa_pd
# Get schema from db
#   pass
# Else import it from schema file (from exact location)
from schema import schema as sh, schema_Id,sheet_name

# Analyzer option. dpa = 0 for Polars or 1 for Pandas
dqa = 0

# Pass file to check
files: list = [
    "C:/Users/KT/source/csv xls xlsx analyzer/backend_node/data/HiH Agricultural Typologies Dataset - Haiti.xlsx",
    "C:/Users/KT/Downloads/HiH Agricultural Typologies - Poverty Dataset - Haiti.csv",
]
f: int = 0
file: str = files[f]
path_test = path.isfile(file)
sep = ","

#indiquer repertoire pour stocker résultats
results_Dir = "../data/"
home = os.path.expanduser("~")
desktop = home + "/Desktop/"

if path_test is True:
    result = (
        dqa_pd(file, sheet_name) if dqa == 1 else dqa_pl(file, sheet_name)
    )
    if "erreur" not in result.keys():
        # transferer DQA résultats dans un fichier json
        respond_tojson = json.dumps(result, indent=4)
        dest = (
            desktop
            + "DQA_"
            + str(result["schema_id"])
            + "_"
            + str(int(datetime.now().timestamp()))
            + ".json"
        )

        with open(dest, "w") as destfile:
            destfile.write(respond_tojson)
    else:
        pass