"""Editable defaults for benchmark scripts."""

DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_CONCURRENCY = 5
DEFAULT_CACHE_MODE = "bypass"
DEFAULT_MAX_RESPONSE_CHARS = 3000
DEFAULT_ALL_COUNTRIES = True
DEFAULT_ALL_LAYERS = True
DEFAULT_MAX_ADMIN1 = -1

API_PATHS = {
    "schools_connectivityconfigs": "/api/statistics/connectivityconfigs/",
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
