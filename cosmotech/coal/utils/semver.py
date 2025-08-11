from packaging.version import Version
import importlib.metadata


def semver_of(package: str) -> Version:
    return Version(importlib.metadata.version(package))
