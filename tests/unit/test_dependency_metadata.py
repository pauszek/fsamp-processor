import tomllib
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _requirement_names(requirements_file: Path) -> set[str]:
    names: set[str] = set()
    for raw_line in requirements_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith(("#", "-")):
            continue
        names.add(canonicalize_name(Requirement(line).name))
    return names


@pytest.mark.parametrize("extra_name", ["dev", "test"])
def test_optional_dependencies_are_covered_by_locked_requirements(extra_name: str) -> None:
    pyproject = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    extra_requirements = {
        canonicalize_name(Requirement(requirement).name)
        for requirement in pyproject["project"]["optional-dependencies"][extra_name]
    }
    locked_requirements = _requirement_names(PROJECT_ROOT / "requirements-dev.txt")

    assert extra_requirements <= locked_requirements
