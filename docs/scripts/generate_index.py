from typing import IO

import mkdocs_gen_files

from cosmotech.coal import __version__

_md_file: IO
with mkdocs_gen_files.open("index.md", "w") as _md_file, open("docs/scripts/index.md.template") as index_template, open(
    "README.md"
) as readme:
    _index: list[str] = index_template.readlines()
    _readme_content = readme.readlines()
    for _line in _index:
        if "--README--" in _line:
            _md_file.writelines(_readme_content[1:])
            continue
        _md_file.write(_line.replace("VERSION", __version__))
