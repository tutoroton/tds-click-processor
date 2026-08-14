"""Every name a module advertises in `__all__` must actually exist.

WHY (prod-readiness audit, 2026-08-14)

Found live in admin-api: `app/sources/schemas.py` listed `PostbackCreate` and
`PostbackResponse` in its `__all__` and neither has ever existed — `git log -S`
finds no commit that defined them. `from … import *` raised AttributeError, and
anything reading `__all__` to describe the module's public surface was told
about symbols that are not there. Broken since the Stage-1 commit, because
nothing looked: admin-api had no lint gate, and ruff's F822 catches exactly this.

This service is clean today — verified, not assumed. The guard is here so it
stays that way, since the same class is invisible until someone writes a star
import or reads the list expecting it to be true.

It IMPORTS each module rather than parsing it: a name can be produced by a
re-export or a conditional import that a static read would miss, and a name that
only *looks* present is the thing being guarded against.
"""

from __future__ import annotations

import ast
import importlib
import pathlib

import pytest

_APP = pathlib.Path(__file__).resolve().parents[2] / "app"


def _modules_declaring_all() -> list[str]:
    """Dotted module names for every file with a top-level `__all__`."""
    found: list[str] = []
    for path in sorted(_APP.rglob("*.py")):
        try:
            tree = ast.parse(path.read_text())
        except SyntaxError:  # pragma: no cover — the syntax gate would fail first
            continue
        for node in tree.body:  # top-level only; a nested __all__ is not an export list
            if isinstance(node, ast.Assign) and any(
                isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets
            ):
                rel = path.relative_to(_APP.parent)
                dotted = str(rel.with_suffix("")).replace("/", ".")
                if dotted.endswith(".__init__"):
                    dotted = dotted[: -len(".__init__")]
                found.append(dotted)
                break
    return found


_MODULES = _modules_declaring_all()


def test_the_probe_found_the_modules_it_is_meant_to_check():
    """POSITIVE CONTROL. If the scan returns nothing — a refactor, a moved
    package, a changed layout — every parametrised case below would silently
    vanish and this file would pass while checking nothing at all."""
    assert len(_MODULES) >= 6, (
        f"only {len(_MODULES)} module(s) with __all__ were found under {_APP}; the scan has "
        "gone blind. Repoint it rather than deleting it."
    )


@pytest.mark.parametrize("dotted", _MODULES)
def test_every_name_in_all_exists(dotted: str):
    module = importlib.import_module(dotted)
    declared = getattr(module, "__all__", None)
    assert declared is not None, f"{dotted}: __all__ vanished between the scan and the import"

    missing = [name for name in declared if not hasattr(module, name)]
    assert not missing, (
        f"{dotted}.__all__ advertises {missing}, which the module does not define. "
        f"`from {dotted} import *` raises AttributeError on those names, and any tool that "
        f"reads __all__ to describe this module's public surface is being told about symbols "
        f"that are not there. Either define them or remove them from __all__."
    )
