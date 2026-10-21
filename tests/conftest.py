"""Shared configuration for the http-protocol suite."""


def pytest_configure(config):
    """Register hivescope's fixtures when hivescope is installed.

    This used to be ``pytest_plugins`` in a ``conftest.py`` at the repository
    ROOT. pytest accepts that key only in a top-level conftest, and a conftest
    at the root puts the root on ``sys.path``, which shadows the installed
    wheel with the working tree. ``import_plugin`` does the same registration
    from here, and the try/except keeps the old contract: a workflow that does
    not install hivescope still runs the unit cells.
    """
    try:
        import hivescope  # noqa: F401
    except ImportError:
        return
    config.pluginmanager.import_plugin("hivescope.pytest_fixtures")
