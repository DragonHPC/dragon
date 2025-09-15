import os
import sys
import inspect
from typing import Any

from sphinx.ext import autodoc

# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
sys.path.insert(0, os.path.abspath("./../src/dragon"))
sys.path.insert(0, os.path.abspath("."))

# -- Project information -----------------------------------------------------

project = "Dragon"
DragonVersion = "0.11"
copyright = "2025, DragonHPC"
author = "Michael Burke, Yian Chen, Eric Cozzi, Zach Crisler, Julius Donnert, Veena Ghorakavi, Nick Hill, Maria Kalantzi, Ben Keen, Kent D. Lee, Pete Mendygral, Davin Potts, Nick Radcliffe, and Colin Wahl"

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "breathe",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autosectionlabel",
    "sphinxfortran.fortran_domain",
    "sphinxfortran.fortran_autodoc",
    "sphinx.ext.linkcode",
    "sphinx.ext.extlinks",
    "sphinx_copybutton",
    "sphinxcontrib.plantuml",
    "sphinx_new_tab_link"
]

tls_verify = False

# autodoc_typehints = 'description'
autodoc_typehints_format = "short"
autodoc_class_signature = "separated"
autodoc_member_order = "bysource"
autosummary_generate = True

# changed this from "any" because we just get too many duplicates that way.
default_role = "code"

intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}
html_show_sphinx = False
new_tab_link_show_external_link_icon = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# Sphinx Fortran options
fortran_src = ["./../src/lib/"]
fortran_ext = ["f90"]

# autosectionlabel options
autosectionlabel_prefix_document = True
autosectionlabel_maxdepth = None

# provides numbering of figures for free.
numfig = True

# temporary as we worj through the docs
exclude_patterns = ["old_devguide/*", "old_cython/*"]

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

# Paths relative to html_static_path
html_css_files = [
    "css/custom.css",
]

# See https://sphinx-rtd-theme.readthedocs.io/en/stable/configuring.html
html_theme_options = {
    "navigation_depth": 6
}

breathe_projects = {"dragon": "../src/doxygen/xml"}
breathe_default_project = "dragon"

add_module_names = False

variables_to_export = [
    "DragonVersion",
]
frozen_locals = dict(locals())
rst_epilog = '\n'.join(map(lambda x: f".. |{x}| replace:: {frozen_locals[x]}", variables_to_export))
del frozen_locals

# Define plantuml compilation
plantuml = f'java -jar {os.path.join(os.getcwd(), "plantuml.jar")}'

# Add an autodoc class that only posts the docstring without function
# names. Useful for autodoc-ing the Dragon CLI commands
class AutoDocstringOnly(autodoc.MethodDocumenter):
    objtype = "docstringonly"

    #do not indent the content
    content_indent = ""

    @classmethod
    def can_document_member(cls, member: Any, membername: str, isattr: bool, parent: Any) -> bool:
        return False

    #do not add a header to the docstring
    def add_directive_header(self, sig):
        pass

autodoc_default_flags = ['members', 'private-members', 'special-members',
                         #'undoc-members',
                         'show-inheritance']

autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'special-members': '__setitem__,__getitem__,__delitem__,__len__,__contains__',
    'undoc-members': False,
    'exclude-members': '__weakref__,__new__,__doc__,__module__,__dict__',
    'show-inheritance': True
}

def setup(app):
    app.add_autodocumenter(AutoDocstringOnly)

def linkcode_resolve(domain, info):
    """
    Returns a link to the source code on GitHub, with appropriate lines highlighted.
    """
    if domain != 'py':
        return None  # Only handle Python objects

    modname = info.get('module')
    fullname = info.get('fullname')

    if not modname or not fullname:
        return None

    try:
        obj = sys.modules[modname]
        for part in fullname.split('.'):
            obj = getattr(obj, part)
    except AttributeError:
        return None

    try:
        filename = inspect.getsourcefile(obj)
        lines, first_line_no = inspect.getsourcelines(obj)
    except TypeError:
        return None
    except OSError:
        # this path happens for Cython code. Will need a solution for this yet.
        return None

    if not filename:
        return None

    # Adjust path to be relative to the repository root
    # This example assumes your project is hosted on GitHub and the source
    # code is in a 'src' directory at the root of your repository.
    # You will need to adapt this part to your specific repository structure.
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    relative_filename = os.path.relpath(filename, repo_root).replace("hpc-pe-dragon-dragon/","") # done like this so builds work on both internal and external repos

    # Construct the GitHub URL
    github_repo_url = "https://github.com/DragonHPC/dragon"
    github_branch = "main" # or "develop", "master", etc.

    return f"{github_repo_url}/blob/{github_branch}/{relative_filename}#L{first_line_no}-L{first_line_no + len(lines) - 1}"

extlinks = {
    'example_benchmark': ('https://github.com/DragonHPC/dragon/tree/main/examples/benchmarks/%s', '%s'),  # use as :example_benchmark:`file.py` to link to examples/benchmarks/file.py
    'example_ai': ('https://github.com/DragonHPC/dragon/tree/main/examples/dragon_ai/%s', '%s'),  # use as :example_ai:`file.py` to link to examples/dragon_ai/file.py
    'example_core': ('https://github.com/DragonHPC/dragon/tree/main/examples/dragon_core/%s', '%s'),
    'example_data': ('https://github.com/DragonHPC/dragon/tree/main/examples/dragon_data/%s', '%s'),
    'example_gs_client': ('https://github.com/DragonHPC/dragon/tree/main/examples/dragon_gs_client/%s', '%s'),
    'example_native': ('https://github.com/DragonHPC/dragon/tree/main/examples/dragon_native/%s', '%s'),
    'example_telemetry': ('https://github.com/DragonHPC/dragon/tree/main/examples/dragon_telemetry/%s', '%s'),
    'example_workflows': ('https://github.com/DragonHPC/dragon/tree/main/examples/workflows/%s', '%s'),
    'example_jupyter': ('https://github.com/DragonHPC/dragon/tree/main/examples/jupyter/%s', '%s'),
    'example_multiprocessing': ('https://github.com/DragonHPC/dragon/tree/main/examples/multiprocessing/%s', '%s'),
    }