"""E3 / F4 — the reserved `tds_*` family is RECORDED, then STRIPPED.

MEASURED FIRST, on deployed staging 2026-09-17 (`tds.events`, 30 days):

    clicks carrying a `tds_` key in extra_params : 1510
    clicks carrying ANY extra_params             : 316 224
    all rows in the window                       : 386 289

and the key-name census names the shape exactly — `landing_url,tds_rc` at 969
rows. So this is a measurement, not a suspicion.

WHY IT LEAKS, and why it is the same defect as `binding_selector` one line
above: `resolve_slots` drops these on the RESOLVED path, while the no-match /
pre-campaign branch rebuilds extras from the RAW query params and re-admits
them. One rule, two paths, covered on one.

🔴 WHY NOT STRIP BLIND. `extra_params.tds_rc` is today the ONLY trace that a
route code was presented and REFUSED — a refused code leaves
`target_selection_path` empty, which is byte-identical to no code at all. So
the FACT is recorded and only the VALUE (a signed token) is dropped.
"""
import pytest

from app.main import _ROUTE_CODE_PRESENTED_KEY, _build_extra_params
from app.router import RESERVED_ROUTING_PREFIX, ROUTE_CODE_PARAM


class TestTheNoMatchPathWhereItActuallyLeaked:
    """`attribution` is None ⇒ extras are rebuilt from RAW query params."""

    def test_the_route_code_VALUE_does_not_survive(self):
        out = _build_extra_params(None, {"tds_rc": "v2.abc.sig", "utm": "x"})
        assert "tds_rc" not in out
        assert "v2.abc.sig" not in out.values()

    def test_but_the_FACT_that_one_was_presented_DOES(self):
        """The whole point of record-then-strip. A blind strip would pass the
        test above and destroy the operator's last handle."""
        out = _build_extra_params(None, {"tds_rc": "v2.abc.sig"})
        assert out[_ROUTE_CODE_PRESENTED_KEY] == "1"

    def test_no_code_presented_means_no_marker(self):
        """The marker must MEAN something — if it appeared unconditionally it
        would carry no information at all."""
        out = _build_extra_params(None, {"utm": "x"})
        assert _ROUTE_CODE_PRESENTED_KEY not in out

    def test_advertiser_data_is_untouched(self):
        """The control that keeps this from being a destructive change."""
        out = _build_extra_params(None, {"tds_rc": "v2.a.b", "utm": "x", "ref": "y"})
        assert out["utm"] == "x"
        assert out["ref"] == "y"

    @pytest.mark.parametrize("key", ["tds_preview", "tds_wall", "tds_anything_new"])
    def test_the_WHOLE_reserved_family_goes_not_just_the_one_we_knew(self, key):
        """A denylist of known names would re-open this for the next `tds_*`
        parameter somebody adds. The prefix is the rule."""
        out = _build_extra_params(None, {key: "1", "utm": "x"})
        assert key not in out
        assert out["utm"] == "x"

    def test_a_non_code_reserved_key_does_NOT_claim_a_code_was_presented(self):
        """`tds_preview` is reserved, but it is not a route code. Marking it as
        one would make the marker lie in the direction nobody audits."""
        out = _build_extra_params(None, {"tds_preview": "1"})
        assert "tds_preview" not in out
        assert _ROUTE_CODE_PRESENTED_KEY not in out

    def test_case_is_not_an_escape_hatch(self):
        out = _build_extra_params(None, {"TDS_RC": "v2.a.b"})
        assert "TDS_RC" not in out
        assert out[_ROUTE_CODE_PRESENTED_KEY] == "1"


class TestTheResolvedPath:
    """`attribution["extras"]` present ⇒ the resolver already ran."""

    def test_a_reserved_key_that_reached_extras_is_still_dropped(self):
        """Defence in depth: ONE rule on BOTH paths, which is exactly what
        `binding_selector` had to be repaired to achieve."""
        out = _build_extra_params({"extras": {"tds_rc": "v2.a.b", "utm": "x"}}, {})
        assert "tds_rc" not in out
        assert out[_ROUTE_CODE_PRESENTED_KEY] == "1"
        assert out["utm"] == "x"


class TestTheMarkerCannotBeForged:
    def test_an_advertiser_supplied_marker_is_dropped(self):
        """Same rule as `_param_rules`: a caller must not be able to write
        provenance. Here the forged value is dropped and NOT replaced, because
        no code was presented."""
        out = _build_extra_params(None, {_ROUTE_CODE_PRESENTED_KEY: "1", "utm": "x"})
        assert _ROUTE_CODE_PRESENTED_KEY not in out
        assert out["utm"] == "x"

    def test_a_forged_marker_does_not_survive_alongside_a_real_code(self):
        out = _build_extra_params(
            None, {_ROUTE_CODE_PRESENTED_KEY: "forged", "tds_rc": "v2.a.b"}
        )
        assert out[_ROUTE_CODE_PRESENTED_KEY] == "1"


class TestTheConstantsAreOneDefinition:
    def test_the_prefix_and_the_key_agree(self):
        """If `ROUTE_CODE_PARAM` ever stopped living under the reserved prefix,
        the strip would silently stop covering it while every test above still
        passed on its own literal."""
        assert ROUTE_CODE_PARAM.startswith(RESERVED_ROUTING_PREFIX)
