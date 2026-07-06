"""OCS Submission Capsule package."""

import os
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("ocs-submission")
except PackageNotFoundError:
    __version__ = "0.0.0+dev"

OUTPUT_DIR = "/results" if os.path.isdir("/results") else "."
