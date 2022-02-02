import nox
import nox_poetry  # type: ignore[import]

nox.options.default_venv_backend = "conda"


@nox_poetry.session(python=["3.9", "3.8", "3.7"])
def tests(session: nox_poetry.Session) -> None:
    session.install("pytest", "pytest-cov", "sqlalchemy_utils", ".")
    session.run("pytest", "--cov=sqlgraphql", "tests/")


@nox_poetry.session(python="3.9")
def lint(session: nox_poetry.Session) -> None:
    session.install(
        "pre-commit",
        "flake8",
        "black",
        "isort",
        "mypy",
        "sqlalchemy",
        "sqlalchemy2-stubs",
        "graphene-stubs",
        "graphql-core",
    )
    session.run("pre-commit", "run", "--all")
