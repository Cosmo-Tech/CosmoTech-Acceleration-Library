from typing import IO

import mkdocs_gen_files
import requirements

_md_file: IO
with mkdocs_gen_files.open("dependencies.md",
                           "w") as _md_file, open("requirements.txt") as _req, open("requirements.doc.txt") as _doc:
    content = ["# List of dependencies", ""]

    _requirements: list[str] = _req.read().splitlines()
    _docs_req: list[str] = _doc.read().splitlines()

    for _r in [_requirements, _docs_req]:
        for _line in _r:
            if not _line:
                content.append("")
            elif _line[0] == "#":
                content.append(_line[1:] + "  ")
            else:
                req = next(requirements.parse(_line))
                _name = req.name
                content.append(f"[ ![PyPI - {_name}]"
                               f"(https://img.shields.io/pypi/l/{_name}"
                               f"?style=for-the-badge"
                               f"&labelColor=informational&label={_name})]"
                               f"(https://pypi.org/project/{_name}/)  ")

    _md_file.writelines(l + "\n" for l in content)
