# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import contextlib
import io
import pathlib
import re
import os

import click

from cosmotech.coal.cli.main import main

commands = {}

def command_tree(obj, base_name):
    commands[base_name] = obj
    if isinstance(obj, click.Group):
        return {name: command_tree(value, f"{base_name} {name}")
                for name, value in obj.commands.items()}


ansi_escape = re.compile(r'(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]')

command_tree(main, "csm-data")

doc_template = """---
hide:
  - toc
description: "Command help: `{command}`"
---
# {command_name}

!!! info "Help command"
    ```text
    --8<-- "{command_help_path}"
    ```
"""

help_folder = pathlib.Path("generated/commands_help")
help_folder.mkdir(parents=True, exist_ok=True)
doc_folder = pathlib.Path("docs/cli")
doc_folder.mkdir(parents=True, exist_ok=True)
for command, cmd in commands.items():
    target_file = help_folder / f"{command}.txt".replace(" ", "/")
    parent_folder = target_file.parent
    parent_folder.mkdir(parents=True, exist_ok=True)
    with open(target_file, "w") as _md_file:
        _md_file.write(f"> {command} --help\n")
        f = io.StringIO()
        with click.Context(cmd, info_name=command) as ctx:
            with contextlib.redirect_stdout(f):
                cmd.get_help(ctx)
        f.seek(0)
        o = ansi_escape.sub('', "".join(f.readlines()))
        _md_file.write(o)
    if os.environ.get("DOC_GENERATE_CLI_CONTENT") is not None:
        doc_file = doc_folder / f"{command}.md".replace(" ", "/")
        parent_folder = doc_file.parent
        parent_folder.mkdir(parents=True, exist_ok=True)
        current_doc = doc_template.format(command=command,
                                          command_name=command.split(" ")[-1],
                                          command_help_path=str(target_file))
        with open(doc_file, "w") as _doc_file:
            _doc_file.write(current_doc)
