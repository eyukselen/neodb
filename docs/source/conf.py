import os
import sys

sys.path.insert(0, os.path.abspath('../../src/'))

from neodb.__version__ import __version__

project = 'neodb'
copyright = '2024, emre'
author = 'emre'
release = __version__


# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
]

templates_path = ['_templates']
exclude_patterns = ['_build', '.idea']

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
# html_theme = 'classic'
html_static_path = ['_static']

html_theme_options = {
    "show_powered_by": False,
    "github_user": "eyukselen",
    "github_repo": "neodb",
    "github_banner": True,
    "show_related": False,
}

html_sidebars = {
    'default': ['localtoc.html', 'genindex.html', 'modindex.html', 'search.html', 'navigation.html'],
}
