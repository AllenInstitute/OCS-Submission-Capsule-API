"""OCS Submission Capsule package."""

import os

__version__ = "0.1.1"

OUTPUT_DIR = "/results" if os.path.isdir("/results") else "."
