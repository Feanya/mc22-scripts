import re


def determine_versionscheme_raemaekers(version: str) -> int:
    """Determine which pattern the version follows. Definitions after Raemaekers et al (2014 and 2017):
    https://ieeexplore.ieee.org/document/6975655

    1 MAJOR.MINOR
    2 MAJOR.MINOR.PATCH
    3 #1 or #2 with nonnumeric chars
    4 MAJOR.MINOR-prerelease
    5 MAJOR.MINOR.PATCH-pre.
    6 Other versioning scheme

    SemVer RegEx from Semantic Versioning 2.0.0:
    https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string
    """
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
