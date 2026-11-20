import tomllib
from typing import IO

import mkdocs_gen_files
import requirements

with open("pyproject.toml", "rb") as _f:
    _pyproject = tomllib.load(_f)

_sections: list[tuple[str, list[str]]] = [
    ("Runtime requirements", _pyproject["project"]["dependencies"]),
    ("Documentation requirements", _pyproject["dependency-groups"]["doc"]),
    ("Test requirements", _pyproject["dependency-groups"]["test"]),
    ("Development requirements", _pyproject["dependency-groups"]["dev"]),
]

_md_file: IO
with mkdocs_gen_files.open("dependencies.md", "w") as _md_file:
    content = ["# List of dependencies", ""]

    for _title, _reqs in _sections:
        content.append(f"## {_title}")
        content.append("")
        for _l in _reqs:
            req = next(requirements.parse(_l))
            _name = req.name
            content.append(
                f"[ ![PyPI - {_name}]"
                f"(https://img.shields.io/pypi/l/{_name}?style=for-the-badge&labelColor=informational&label={_name})]"
                f"(https://pypi.org/project/{_name}/)  "
            )
        content.append("")

    _md_file.writelines(_l + "\n" for _l in content)
