from typing import IO

import mkdocs_gen_files
import requirements

_md_file: IO
with (mkdocs_gen_files.open("dependencies.md", "w") as _md_file,
      open("requirements.txt") as _req,
      open("requirements.doc.txt") as _doc_req,
      open("requirements.test.txt") as _test_req,
      open("requirements.extra.txt") as _extra_req):
    content = ["# List of dependencies", ""]

    _requirements: list[str] = _req.read().splitlines()
    _doc_requirements: list[str] = _doc_req.read().splitlines()
    _test_requirements: list[str] = _test_req.read().splitlines()
    _extra_requirements: list[str] = _extra_req.read().splitlines()

    for _r in [_requirements, _doc_requirements, _extra_requirements, _test_requirements]:
        for _l in _r:
            if not _l:
                content.append("")
            elif _l[0] == "#":
                content.append(_l[1:] + "  ")
            else:
                req = next(requirements.parse(_l))
                _name = req.name
                content.append(
                    f"[ ![PyPI - {_name}]"
                    f"(https://img.shields.io/pypi/l/{_name}?style=for-the-badge&labelColor=informational&label={_name})]"
                    f"(https://pypi.org/project/{_name}/)  ")

    _md_file.writelines(_l + "\n" for _l in content)

