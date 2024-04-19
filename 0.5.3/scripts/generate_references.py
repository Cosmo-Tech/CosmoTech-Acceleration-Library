import os

import mkdocs_gen_files
from griffe.dataclasses import Alias
from griffe.dataclasses import Module

pyhand = mkdocs_gen_files.config['plugins']['mkdocstrings'].get_handler('python')
module_name = 'CosmoTech_Acceleration_Library'

griffed_module = pyhand.collect(module_name, {})


def yield_module_member(module: Module) -> list[bool, str]:
    for sub_module in module.modules.values():
        yield from yield_module_member(sub_module)
    if len(m_classes := module.classes) > 0:
        for c in m_classes.values():
            if isinstance(c, Alias):
                # escape the classes import which are alias of class
                continue
            yield True, c.path
    if len(module.functions) > 0 and all([not isinstance(f, Alias) for f in module.functions.values()]):
        yield False, module.path


depth = 0
parents = {}
for is_class, identifier in yield_module_member(griffed_module):
    parent, *sub = identifier.rsplit('.', depth)
    if is_class:
        parent, *sub = identifier.rsplit('.', depth + 1)
    parents.setdefault(parent, set())
    if sub:
        parents[parent].add(sub[0])
    else:
        parents[parent].add(parent)

# gen md files
with open('docs/scripts/generic_ref.md.template') as f:
    generic_template_ref = f.read()

mk_nav = mkdocs_gen_files.Nav()
for nav, file_set in parents.items():
    nav_root = ['References']
    nav_root.extend(n for n in nav.split('.')[1:] if n)
    file_name = '/'.join(nav.split('.')[1:]) + '.md'
    mk_nav[nav_root] = file_name
    with mkdocs_gen_files.open(os.path.join('references', file_name), 'w') as f:
        f.write(f'# {nav}')
        f.write('\n')
        for filz in sorted(file_set):
            _content = ""
            if filz != nav:
                _content = generic_template_ref.replace('%%IDENTIFIER%%', '.'.join([nav, filz]))
            else:
                _content = generic_template_ref.replace('%%IDENTIFIER%%', filz)
            f.write(_content)
            f.write('\n')

with mkdocs_gen_files.open("references/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(mk_nav.build_literate_nav())
