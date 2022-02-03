import re


def determine_versionscheme(version: str) -> int:
    # match SEMVER
    if re.fullmatch(pattern=
                    "^(0|[1-9]\d*)\.(0|[1-9]\d*)$",
                    string=version) is not None:
        return 1
    if re.fullmatch(pattern=
                    "^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$",
                    string=version) is not None:
        return 2
    if re.fullmatch(pattern=
                    "^(0|[1-9a-zA-Z]*)\.(0|[1-9a-zA-Z]*)(\.(0|[1-9a-zA-Z]\d*))?$",
                    string=version) is not None:
        return 3
    if re.fullmatch(pattern=
                    "^(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
                    "(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$",
                    string=version) is not None:
        return 4
    if re.fullmatch(pattern=
                    "^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
                    "(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$",
                    string=version) is not None:
        return 5
    else:
        return 6
