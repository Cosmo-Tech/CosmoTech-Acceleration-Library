from typing import IO

import mkdocs_gen_files
import CosmoTech_Acceleration_Library

_md_file: IO
with mkdocs_gen_files.open("index.md", "w") as _md_file, open("scripts/index.md.template") as index_template:
    _index: list[str] = index_template.readlines()
    for _line in _index:
        _md_file.write(_line.replace("VERSION", CosmoTech_Acceleration_Library.__version__))
