import polars as pl

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
        "type": "string",
        "requis": True,
        "minimum": 3,
        "maximum": 50,
        "pattern": any((char.isalpha() or char.isspace()) for char in "Stringvalue"),
    },
    "comm_code": {
        "type": "number",
        "requis": True,
        "minimum": 3,
        "maximum": 50,
        "pattern": any((char.isdigit() or char.isspace()) for char in "Stringvalue"),
    },
    "gid_3": {
        "type": "string",
        "requis": True,
        "minimum": 3,
        "maximum": 50,
        "pattern": 'starts_with("HTI.")',
    },
    "typology_classes_kmedians": {
        "type": "string",
        "requis": False,
        "minimum": 10,
        "maximum": 50,
        "pattern": any((char.isalpha() or char.isspace()) for char in "Stringvalue"),
    },
    "classes_typologiques_kmediansx": {
        "type": "string",
        "requis": False,
        "minimum": 3,
        "maximum": 50,
        "pattern": any((char.isalpha() or char.isspace()) for char in "Stringvalue"),
    },
    "poverty": {
        "type": "float",
        "requis": False,
        "minimum": 3,
        "maximum": 50,
    },
}

#'ends_with("HTI.")', .str.contains("(?i)AA")
