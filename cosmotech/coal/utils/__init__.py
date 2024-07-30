from CosmoTech_Acceleration_Library import __version__

WEB_DOCUMENTATION_ROOT = f"https://cosmo-tech.github.io/CosmoTech-Acceleration-Library/{__version__}/"


def strtobool(string: str) -> bool:
    if string.lower() in ["y", "yes", "t", "true", "on", "1"]:
        return True
    if string.lower() in ["n", "no", "f", "false", "off", "0"]:
        return False
    raise ValueError(f'"{string} is not a recognized truth value')
