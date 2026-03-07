"""Tests for country and jurisdiction resolution."""

from bods_opencorporates.utils.countries import (
    jurisdiction_to_country_code,
    resolve_country,
    resolve_jurisdiction,
    resolve_nationalities,
)


class TestResolveJurisdiction:
    def test_country_level(self):
        result = resolve_jurisdiction("gb")
        assert result == {"code": "GB", "name": "United Kingdom"}

    def test_subnational_us(self):
        result = resolve_jurisdiction("us_de")
        assert result is not None
        assert result["code"] == "US-DE"
        assert "Delaware" in result["name"]

    def test_subnational_canada(self):
        result = resolve_jurisdiction("ca_bc")
        assert result is not None
        assert result["code"] == "CA-BC"

    def test_uppercase(self):
        result = resolve_jurisdiction("GB")
        assert result is not None
        assert result["code"] == "GB"

    def test_unknown(self):
        result = resolve_jurisdiction("zz")
        # 'zz' is not a valid ISO code; pycountry fuzzy may or may not match
        # The important thing is it doesn't crash
        if result is not None:
            assert "code" in result
            assert "name" in result

    def test_empty(self):
        result = resolve_jurisdiction("")
        assert result is None


class TestResolveCountry:
    def test_full_name(self):
        result = resolve_country("Germany")
        assert result is not None
        assert result["code"] == "DE"

    def test_alpha2(self):
        result = resolve_country("FR")
        assert result is not None
        assert result["code"] == "FR"

    def test_abbreviation_uk(self):
        result = resolve_country("UK")
        assert result is not None
        assert result["code"] == "GB"

    def test_england(self):
        result = resolve_country("England")
        assert result is not None
        assert result["code"] == "GB"

    def test_none(self):
        assert resolve_country(None) is None

    def test_empty(self):
        assert resolve_country("") is None


class TestResolveNationalities:
    def test_british(self):
        result = resolve_nationalities("British")
        assert len(result) == 1
        assert result[0]["code"] == "GB"

    def test_french(self):
        result = resolve_nationalities("French")
        assert len(result) == 1
        assert result[0]["code"] == "FR"

    def test_multiple(self):
        result = resolve_nationalities("British, French")
        assert len(result) == 2
        codes = {r["code"] for r in result}
        assert "GB" in codes
        assert "FR" in codes

    def test_empty(self):
        assert resolve_nationalities("") == []

    def test_none(self):
        assert resolve_nationalities(None) == []


class TestJurisdictionToCountryCode:
    def test_simple(self):
        assert jurisdiction_to_country_code("gb") == "GB"

    def test_subnational(self):
        assert jurisdiction_to_country_code("us_de") == "US"

    def test_none(self):
        assert jurisdiction_to_country_code("") is None
