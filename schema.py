import polars as pl

schema_Id = "Test_1001"
sheet_name = "Data"
schema = {
    "department": {
        "type": "string",
        "requis": True,
        "enum": [
            "Ouest",
            "Sud",
            "Sud-Est",
            "Artibonite",
            "Centre",
            "Grand Anse",
            "Nippes",
            "Nord",
            "Nord Ouest",
            "Nord Est",
        ],
    },
    "commune": {
        "type": "str",
        "requis": True,
        "unique": True,
        "minimum": 3,
        "maximum": 50,
        "pattern": lambda cell: all(
            (char.isalpha() or char.isspace()) for char in cell
        ),
    },
    "comm_code": {
        "type": "nombre",
        "requis": True,
        "unique": True,
        "minimum": 100,
        "maximum": 900,
    },
    "gid_3": {
        "type": "string",
        "requis": True,
        "minimum": 3,
        "maximum": 50,
        "pattern": lambda cell: cell.startswith("HTI."),
    },
    "typology_classes_kmedians": {
        "type": "string",
        "requis": False,
        "minimum": 10,
        "maximum": 50,
        "pattern": lambda cell: all(
            (char.isalpha() or char.isspace()) for char in cell
        ),
    },
    "classes_typologiques_kmedians": {
        "type": "string",
        "requis": False,
        "minimum": 3,
        "maximum": 50,
        "pattern": lambda cell: all(
            (char.isalpha() or char.isspace()) for char in cell
        ),
    },
    "poverty": {
        "type": "float",
        "requis": False,
        "minimum": 0,
        "maximum": 1,
    },
    "proportion": {
        "type": "float",
        "requis": False,
        "minimum": 0,
        "maximum": 1,
    },
    "date": {
        "type": "date",
        "requis": False,

    },
}

#'ends_with("HTI.")', .str.contains("(?i)AA")
