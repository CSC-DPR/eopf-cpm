# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------
from eopf import __version__

project = "EOPF Core Python Modules"
copyright = "2022, ESA"
author = "CSGroup"

# The full version, including alpha/beta/rc tags
release = __version__


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "jupyter_sphinx",
    "sphinx_multiversion",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_book_theme"
html_theme_options = dict(
    repository_url="https://github.com/CSC-DPR/eopf-cpm",
    repository_branch="main",
    use_repository_button=True,
    use_edit_page_button=False,
    home_page_in_toc=False,
    logo_only=True,
)
html_logo = "_static/logo.jpg"
html_title = "EOPF - Core Python Modules"
# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_sidebars = {"**": ["sidebar-logo.html", "search-field.html", "sbt-sidebar-nav.html", "versioning.html"]}

# multiple versions options
smv_remote_whitelist = r"^.*$"
smv_tag_whitelist = r"^v\d+\.\d+\.\d+$"
smv_branch_whitelist = r"^(main|develop).*$"

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "numpy": ("https://numpy.org/doc/stable", None),
    "matplotlib": ("https://matplotlib.org/stable/", None),
    "dask": ("https://docs.dask.org/en/latest", None),
    "rasterio": ("https://rasterio.readthedocs.io/en/latest", None),
    "xarray": ("https://docs.xarray.dev/en/stable/", None),
    "zarr": ("https://zarr.readthedocs.io/en/stable/", None),
}
