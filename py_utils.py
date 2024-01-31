import polars as pl


def polarsType(col: str):
    if col is None:
        return
    else:
        mapType = {
            "bool": pl.Boolean,
            "boolean": pl.Boolean,
            "str": pl.Utf8,
            "string": pl.Utf8,
            "number": pl.Int32,
            "nombre": pl.Int32,
            "numeric": pl.Int32,
            "entier": pl.Int32,
            "int": pl.Int32,
            "int8": pl.Int8,
            "int16": pl.Int16,
            "int32": pl.Int32,
            "int64": pl.Int64,
            "float": pl.Float32,
            "float32": pl.Float32,
            "float64": pl.Float64,
            "float64": pl.Decimal,
            "date": pl.Date,
            "datetime": pl.Datetime,
            "time": pl.Time,
            "enum": pl.Enum,
        }
        return mapType.get(col.lower())