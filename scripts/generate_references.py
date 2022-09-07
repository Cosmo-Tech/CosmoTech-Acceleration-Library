import mkdocs_gen_files
import os
from griffe.dataclasses import Kind
from griffe.dataclasses import Module
from griffe.dataclasses import Alias


pyhand = mkdocs_gen_files.config['plugins']['mkdocstrings'].get_handler('python')
module_name = 'CosmoTech_Acceleration_Library'

griffed_module = pyhand.collect(module_name, {})


def yield_module_member(module: Module) -> list:
    for sub_module in module.modules.values():
        yield from yield_module_member(sub_module)
    if len(m_classes := module.classes) > 0:
        for c in m_classes.values():
            if isinstance(c, Alias):
                # escape the classes import which are alias of class
                continue
            yield c.path
    if len(module.functions) > 0 and all([not isinstance(f, Alias) for f in module.functions.values()]):
        yield module.path


depth = 1
parents = {}
for identifier in yield_module_member(griffed_module):
    parent, *sub = identifier.rsplit('.', depth)
    _, parent = parent.split('.', 1)  # remove module name
    if parent not in parents.keys():
        parents[parent] = set()
    parents[parent].add(sub[0])

# gen md files
with open('docs/scripts/generic_ref.md.template') as f:
    generic_template_ref = f.read()

mk_nav = mkdocs_gen_files.Nav()
for nav, file_set in parents.items():
    nav_root = ['References']
    nav_root.extend(nav.split('.'))
    file_name = '.'.join([nav, 'md'])

    mk_nav[nav_root] = file_name
    with mkdocs_gen_files.open(os.path.join('references', file_name), 'w') as f:
        f.write(f'# {nav}')
        f.write('\n')
        for filz in file_set:
            f.write(generic_template_ref.replace('%%IDENTIFIER%%', '.'.join([module_name, nav, filz])))
            f.write('\n')

with mkdocs_gen_files.open("references/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(mk_nav.build_literate_nav())
