# coding: latin-1

import polars as pl


def df2x_dict(df: pl.DataFrame) -> dict:
    """
    Recevoir un df n x 2 et le transformer en dictionnaire
    {cle:valeur}
    """
    ntypes = df.height
    return (
        {df[0, 0]: df[0, 1]}
        if ntypes == 1
        else {df[i, 0]: df[i, 1] for i in range(ntypes)}
    )


def polarsType(col: str, r=0):
    """
    Recevoir un string: un nom de colonne du schéma.
    Si r=0,la classe DataType Polars correspondant
    au type indiqué en langage naturel dans le schema (e.g. pl.Utf8) sera retourné.
    Sinon, r != 0, le nom du type correpondant sera retourné en texte (eg. 'string')
    args:
        col: string, le nom de la colonne
        r: int indiquant le format du type à retourner
    return:
        string ou une classe DataType de Polars
    """
    if col is None:
        return
    else:
        plType = {
            "bool": [pl.Boolean, "boolean"],
            "boolean": [pl.Boolean, "boolean"],
            "str": [pl.Utf8, "string"],
            "string": [pl.Utf8, "string"],
            "number": [pl.Int32, "int32"],
            "nombre": [pl.Int32, "int32"],
            "numeric": [pl.Int32, "int32"],
            "entier": [pl.Int32, "int32"],
            "int": [pl.Int32, "int32"],
            "int8": [pl.Int8, "int8"],
            "int16": [pl.Int16, "int16"],
            "int32": [pl.Int32, "int32"],
            "int64": [pl.Int64, "int64"],
            "float": [pl.Float32, "float32"],
            "float32": [pl.Float32, "float32"],
            "float64": [pl.Float64, "float64"],
            "decimal": [pl.Decimal, "decimal"],
            "date": [pl.Date, "date"],
            "datetime": [pl.Datetime, "datetime"],
            "time": [pl.Time, "time"],
            "enum": [pl.Enum, "enum"],
        }
        return plType.get(col.lower())[0] if r == 0 else plType.get(col.lower())[1]


def pandasType(col: str) -> str:
    if col is None:
        return
    else:
        pdType = {
            "bool": "boolean",
            "boolean": "boolean",
            "str": "str",
            "string": "str",
            "number": "str",
            "nombre": "int",
            "numeric": "int",
            "entier": "int",
            "integer": "int",
            "int": "int",
            "int8": "int8",
            "int16": "int16",
            "int32": "int32",
            "int64": "int64",
            "float": "float",
            "float32": "float32",
            "float64": "float64",
            "decimal": "float",
            # "date": "date",
            # "datetime": pl.Datetime,
            # "time": pl.Time,
            # "enum": pl.Enum,
        }
        return pdType.get(col.lower())