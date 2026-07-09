"""Editable defaults for benchmark scripts."""

DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_CONCURRENCY = 5
DEFAULT_CACHE_MODE = "bypass"
DEFAULT_MAX_RESPONSE_CHARS = 0
DEFAULT_ALL_COUNTRIES = True
DEFAULT_ALL_LAYERS = False
DEFAULT_MAX_ADMIN1 = -1

API_PATHS = {
    "schools_connectivityconfigs": "/api/statistics/connectivityconfigs/",
    "entities_connectivityconfigs": "/api/v2/entities/connectivityconfigs/",
    "schools_tiles_connectivity": "/api/locations/schools/tiles/connectivity/",
    "entities_tiles_connectivity": "/api/v2/entities/tiles/connectivity/",
    "schools_tiles_connectivity_status": "/api/locations/schools/tiles/connectivity_status/",
    "entities_tiles_connectivity_status": "/api/v2/entities/tiles/connectivity_status/",
    "schools_layers_map": "/api/accounts/layers/{layer_id}/map/",
    "entities_layers_map": "/api/v2/entities/layers/map/",
    "schools_layers_info": "/api/accounts/layers/{layer_id}/info/",
    "entities_layers_info": "/api/v2/entities/layers/info/",
    "schools_layers_published": "/api/accounts/layers/PUBLISHED/",
    "entities_layers_published": "/api/v2/entities/layers/PUBLISHED/",
}

BENCHMARKS = {
    "schools_connectivityconfigs": {
        "api_key": "schools_connectivityconfigs",
        "output_path": "connectivityconfigs-all-country-admin1-benchmark.xlsx",
        "concurrency": DEFAULT_CONCURRENCY,
        "cache_mode": DEFAULT_CACHE_MODE,
        "max_response_chars": DEFAULT_MAX_RESPONSE_CHARS,
        "all_countries": DEFAULT_ALL_COUNTRIES,
        "all_layers": DEFAULT_ALL_LAYERS,
        "max_admin1": DEFAULT_MAX_ADMIN1,
    },
}

ENTITY_COMPARISON = {
    "output_path": "old-vs-entity-api-benchmark.xlsx",
    "concurrency": DEFAULT_CONCURRENCY,
    "cache_mode": DEFAULT_CACHE_MODE,
    "max_response_chars": DEFAULT_MAX_RESPONSE_CHARS,
    "all_countries": DEFAULT_ALL_COUNTRIES,
    "all_layers": DEFAULT_ALL_LAYERS,
    "max_admin1": DEFAULT_MAX_ADMIN1,
    "flush_every": 50,
    "entity_type_codes": ("school", "health"),
    "tile_coordinates": (
        {"z": 2, "x": 2, "y": 1},
        {"z": 4, "x": 8, "y": 6},
    ),
}

DEFAULT_COUNTRY_CODES = (
    "cn", "ss", "ke", "np", "sl", "mm", "lb", "sy", "sd", "iq", "tr", "jo", "ph", "ps", "ye", "ng",
    "af", "pk", "kh", "id", "cd", "kz", "so", "za", "rw", "et", "lr", "kg", "bo", "ht", "mw", "in",
    "bf", "zm", "ly", "lk", "ci", "mz", "bd", "gn", "ug", "zw", "mg", "tz", "ao", "la", "sn", "ml",
    "ne", "cm", "tn", "dj", "td", "jm", "bb", "kp", "pg", "th", "vn", "ua", "bi", "er", "ar", "br",
    "ec", "mx", "co", "cf", "gh", "tg", "gw", "bj", "mr", "my", "ir", "eg", "cg", "gq", "cv", "ga",
    "st", "gm", "xk", "na", "sz", "bg", "rs", "tj", "md", "ro", "by", "ls", "uz", "mn", "bt", "mv",
    "al", "mk", "me", "tm", "cl", "hr", "sv", "do", "pe", "am", "az", "gr", "bz", "cr", "gt", "pa",
    "py", "hn", "ma", "dz", "ba", "bw", "km", "cu", "ge", "gy", "ni", "sa", "tl", "uy", "ve", "cz",
)
