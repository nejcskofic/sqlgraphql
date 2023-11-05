import os

import nox  # type:ignore[import-not-found]

nox.options.default_venv_backend = "conda"
os.environ.update({"PDM_IGNORE_SAVED_PYTHON": "1"})
MIN_COVERAGE = 65


@nox.session(python=["3.10", "3.11"])
def test(session: nox.Session) -> None:
    session.env.pop(
        "VIRTUAL_ENV", None
    )  # nox does not clear this and pdm takes this before CONDA_PREFIX
    session.run_always(
        "pdm",
        "install",
        "-G",
        "sqlalchemy-utils",
        "-dG",
        "test",
        "--check",
        "--no-editable",
        "-q",
        external=True,
    )
    session.run("pytest", "--cov=sqlgraphql", f"--cov-fail-under={MIN_COVERAGE}", "tests/")


@nox.session(python="3.10")
def lint(session: nox.Session) -> None:
    session.env.pop(
        "VIRTUAL_ENV", None
    )  # nox does not clear this and pdm takes this before CONDA_PREFIX
    session.run_always(
        "pdm",
        "install",
        "-G",
        "sqlalchemy-utils",
        "-dG",
        "lint",
        "--check",
        "--no-self",
        external=True,
    )
    session.run("pre-commit", "run", "--all", external=True)
