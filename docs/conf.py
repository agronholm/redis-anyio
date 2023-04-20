#!/usr/bin/env python3
from importlib.metadata import version as get_version

from packaging.version import parse

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx_autodoc_typehints",
]

source_suffix = ".rst"
master_doc = "index"
project = "Redis-AnyIO"
author = "Alex Grönholm"
copyright = "2023, " + author

v = parse(get_version("redis-anyio"))
version = v.base_version
release = v.public

language = "en"

exclude_patterns = ["_build"]
pygments_style = "sphinx"
autodoc_default_options = {"members": True}
todo_include_todos = False

html_theme = "sphinx_rtd_theme"
htmlhelp_basename = "redisanyiodoc"

intersphinx_mapping = {"python": ("https://docs.python.org/3/", None)}
