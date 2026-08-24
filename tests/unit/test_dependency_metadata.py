import tomllib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_safety_is_not_declared_as_a_development_dependency() -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    development_dependencies = pyproject["project"]["optional-dependencies"]["dev"]

    assert not any(
        dependency.casefold().startswith("safety") for dependency in development_dependencies
    )
