"""Allow ``python -m ocs_submission`` as an alternative to the ``ocs-submission`` console script."""

from .main import main

if __name__ == "__main__":
    main()
