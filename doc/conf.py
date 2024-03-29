"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documentation builds.
"""

from documenteer.conf.pipelinespkg import *  # noqa: F403, import *

project = "ctrl_bps_htcondor"
html_theme_options["logotext"] = project  # noqa: F405, unknown name
html_title = project
html_short_title = project
doxylink = {}
exclude_patterns = ["changes/*"]

# Add intersphinx entries.
intersphinx_mapping["intersphinx"] = ("https://networkx.org/documentation/stable/", None)  # noqa
intersphinx_mapping["lsst"] = ("https://pipelines.lsst.io/v/daily/", None)  # noqa

nitpick_ignore_regex = [
    ("py:obj", "htcondor\\..*"),
]
