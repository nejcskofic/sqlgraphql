import nox
import nox_poetry

nox.options.default_venv_backend = "conda"


@nox_poetry.session(python=["3.9", "3.8", "3.7"])
def tests(session: nox_poetry.Session) -> None:
    session.install("pytest", "pytest-cov", ".")
    session.run("pytest", "--cov=sqlgraphql", "tests/")


@nox_poetry.session(python="3.9")
def lint(session: nox_poetry.Session) -> None:
    session.install("pre-commit")
    session.run("pre-commit", "run", "--all")
