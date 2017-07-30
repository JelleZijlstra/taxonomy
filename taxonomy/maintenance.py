'''Maintenance scripts'''

import re
import sys

from .db.ehphp import call_ehphp
from .db.models import Name

def cite_exists(cite):
    return call_ehphp('exists', {'0': cite})

def get_target(cite):
    return call_ehphp('getTarget', {'0': cite})

def may_be_citation(cite):
    '''Checks whether a citation may be a catalog ID'''
    return '.' not in cite or re.search(r"\.[a-z]+$", cite)

def must_be_citation(cite):
    return re.search(r"\.[a-z]+$", cite)

def check_refs():
    for name in Name.select():
        # if there is an original_citation, check whether it is valid
        if name.original_citation:
            if not cite_exists(name.original_citation):
                print("Name:", name.description())
                print("Warning: invalid original citation:", name.original_citation)
        elif name.verbatim_citation and may_be_citation(name.verbatim_citation):
            if cite_exists(name.verbatim_citation):
                name.original_citation = name.verbatim_citation
                name.verbatim_citation = None
                name.save()
            elif must_be_citation(name.verbatim_citation):
                print("Name:", name.description())
                print("Warning: invalid citation:", name.verbatim_citation)

def resolve_redirects():
    for name in Name.select():
        if name.original_citation:
            target = get_target(name.original_citation)
            if target is not None:
                if target != name.original_citation:
                    print('Fixing redirect for %s: %s -> %s' %
                          (name, name.original_citation, target))
                    name.original_citation = target
                    name.save()
            else:
                print('WARNING: citation for %s does not exist: %s' %
                      (name, name.original_citation))

scripts = {
    'check_refs': check_refs,
    'resolve_redirects': resolve_redirects,
}

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(sys.argv[0] + ": error: no argument given")
    script = scripts[sys.argv[1]]
    script()
