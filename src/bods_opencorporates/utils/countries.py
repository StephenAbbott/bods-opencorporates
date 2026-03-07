"""Country and jurisdiction code resolution using pycountry.

OpenCorporates uses jurisdiction codes like 'gb', 'us_de', 'dk', 'fr'.
BODS requires ISO 3166-1 alpha-2 country codes (e.g. 'GB') or
ISO 3166-2 subdivision codes (e.g. 'US-DE') for jurisdictions.
"""

from __future__ import annotations

import logging
import re

import pycountry

logger = logging.getLogger(__name__)

# Demonym to ISO country code mapping for resolving nationality strings
DEMONYM_TO_COUNTRY: dict[str, str] = {
    "afghan": "AF",
    "albanian": "AL",
    "algerian": "DZ",
    "american": "US",
    "andorran": "AD",
    "angolan": "AO",
    "argentine": "AR",
    "argentinian": "AR",
    "armenian": "AM",
    "australian": "AU",
    "austrian": "AT",
    "azerbaijani": "AZ",
    "bahamian": "BS",
    "bahraini": "BH",
    "bangladeshi": "BD",
    "barbadian": "BB",
    "belarusian": "BY",
    "belgian": "BE",
    "belizean": "BZ",
    "beninese": "BJ",
    "bermudian": "BM",
    "bhutanese": "BT",
    "bolivian": "BO",
    "bosnian": "BA",
    "brazilian": "BR",
    "british": "GB",
    "bruneian": "BN",
    "bulgarian": "BG",
    "burmese": "MM",
    "burundian": "BI",
    "cambodian": "KH",
    "cameroonian": "CM",
    "canadian": "CA",
    "cape verdean": "CV",
    "chadian": "TD",
    "chilean": "CL",
    "chinese": "CN",
    "colombian": "CO",
    "congolese": "CG",
    "costa rican": "CR",
    "croatian": "HR",
    "cuban": "CU",
    "cypriot": "CY",
    "czech": "CZ",
    "danish": "DK",
    "djiboutian": "DJ",
    "dominican": "DO",
    "dutch": "NL",
    "ecuadorian": "EC",
    "egyptian": "EG",
    "emirati": "AE",
    "english": "GB",
    "eritrean": "ER",
    "estonian": "EE",
    "ethiopian": "ET",
    "fijian": "FJ",
    "filipino": "PH",
    "finnish": "FI",
    "french": "FR",
    "gabonese": "GA",
    "gambian": "GM",
    "georgian": "GE",
    "german": "DE",
    "ghanaian": "GH",
    "greek": "GR",
    "grenadian": "GD",
    "guatemalan": "GT",
    "guinean": "GN",
    "guyanese": "GY",
    "haitian": "HT",
    "honduran": "HN",
    "hungarian": "HU",
    "icelandic": "IS",
    "indian": "IN",
    "indonesian": "ID",
    "iranian": "IR",
    "iraqi": "IQ",
    "irish": "IE",
    "israeli": "IL",
    "italian": "IT",
    "ivorian": "CI",
    "jamaican": "JM",
    "japanese": "JP",
    "jordanian": "JO",
    "kazakh": "KZ",
    "kenyan": "KE",
    "korean": "KR",
    "south korean": "KR",
    "north korean": "KP",
    "kuwaiti": "KW",
    "kyrgyz": "KG",
    "laotian": "LA",
    "latvian": "LV",
    "lebanese": "LB",
    "liberian": "LR",
    "libyan": "LY",
    "liechtensteiner": "LI",
    "lithuanian": "LT",
    "luxembourgish": "LU",
    "macedonian": "MK",
    "malagasy": "MG",
    "malawian": "MW",
    "malaysian": "MY",
    "maldivian": "MV",
    "malian": "ML",
    "maltese": "MT",
    "mauritanian": "MR",
    "mauritian": "MU",
    "mexican": "MX",
    "moldovan": "MD",
    "monacan": "MC",
    "mongolian": "MN",
    "montenegrin": "ME",
    "moroccan": "MA",
    "mozambican": "MZ",
    "namibian": "NA",
    "nepalese": "NP",
    "new zealander": "NZ",
    "nicaraguan": "NI",
    "nigerian": "NG",
    "nigerien": "NE",
    "norwegian": "NO",
    "omani": "OM",
    "pakistani": "PK",
    "panamanian": "PA",
    "paraguayan": "PY",
    "peruvian": "PE",
    "polish": "PL",
    "portuguese": "PT",
    "qatari": "QA",
    "romanian": "RO",
    "russian": "RU",
    "rwandan": "RW",
    "salvadoran": "SV",
    "saudi": "SA",
    "saudi arabian": "SA",
    "scottish": "GB",
    "senegalese": "SN",
    "serbian": "RS",
    "sierra leonean": "SL",
    "singaporean": "SG",
    "slovak": "SK",
    "slovenian": "SI",
    "somali": "SO",
    "south african": "ZA",
    "spanish": "ES",
    "sri lankan": "LK",
    "sudanese": "SD",
    "surinamese": "SR",
    "swedish": "SE",
    "swiss": "CH",
    "syrian": "SY",
    "taiwanese": "TW",
    "tajik": "TJ",
    "tanzanian": "TZ",
    "thai": "TH",
    "togolese": "TG",
    "trinidadian": "TT",
    "tunisian": "TN",
    "turkish": "TR",
    "turkmen": "TM",
    "ugandan": "UG",
    "ukrainian": "UA",
    "uruguayan": "UY",
    "uzbek": "UZ",
    "venezuelan": "VE",
    "vietnamese": "VN",
    "welsh": "GB",
    "yemeni": "YE",
    "zambian": "ZM",
    "zimbabwean": "ZW",
}


def resolve_jurisdiction(oc_jurisdiction_code: str) -> dict | None:
    """Convert an OpenCorporates jurisdiction code to a BODS jurisdiction object.

    OpenCorporates uses codes like:
        - 'gb' → country-level (Great Britain)
        - 'us_de' → subnational (US, Delaware)
        - 'ca_bc' → subnational (Canada, British Columbia)

    Returns:
        {"code": "GB", "name": "United Kingdom"} or
        {"code": "US-DE", "name": "Delaware"} or
        None if unresolvable.
    """
    if not oc_jurisdiction_code:
        return None

    code = oc_jurisdiction_code.strip().lower()

    # Subnational: 'us_de' → 'US-DE'
    if "_" in code:
        parts = code.split("_", 1)
        country_alpha2 = parts[0].upper()
        subdivision_code = f"{country_alpha2}-{parts[1].upper()}"

        # Look up the subdivision in pycountry
        subdivision = pycountry.subdivisions.get(code=subdivision_code)
        if subdivision:
            return {
                "code": subdivision_code,
                "name": subdivision.name,
            }

        # Fallback: return with the country code if subdivision not found
        country = pycountry.countries.get(alpha_2=country_alpha2)
        if country:
            return {
                "code": subdivision_code,
                "name": f"{country.name} ({parts[1].upper()})",
            }

        return None

    # Country-level: 'gb' → 'GB'
    alpha2 = code.upper()
    country = pycountry.countries.get(alpha_2=alpha2)
    if country:
        return {
            "code": alpha2,
            "name": country.name,
        }

    # Try fuzzy matching as last resort
    try:
        results = pycountry.countries.search_fuzzy(code)
        if results:
            return {
                "code": results[0].alpha_2,
                "name": results[0].name,
            }
    except LookupError:
        pass

    logger.warning("Could not resolve jurisdiction code: %s", oc_jurisdiction_code)
    return None


def resolve_country(country_text: str | None) -> dict | None:
    """Resolve a free-text country name to a BODS country object.

    Args:
        country_text: A country name like "United Kingdom", "UK", "Germany", etc.

    Returns:
        {"code": "GB", "name": "United Kingdom"} or None.
    """
    if not country_text or not country_text.strip():
        return None

    text = country_text.strip()

    # Direct alpha-2 code
    if len(text) == 2:
        country = pycountry.countries.get(alpha_2=text.upper())
        if country:
            return {"code": country.alpha_2, "name": country.name}

    # Direct alpha-3 code
    if len(text) == 3:
        country = pycountry.countries.get(alpha_3=text.upper())
        if country:
            return {"code": country.alpha_2, "name": country.name}

    # Common abbreviations
    abbreviations = {
        "uk": "GB",
        "usa": "US",
        "uae": "AE",
        "england": "GB",
        "scotland": "GB",
        "wales": "GB",
        "northern ireland": "GB",
        "great britain": "GB",
    }
    lower_text = text.lower()
    if lower_text in abbreviations:
        code = abbreviations[lower_text]
        country = pycountry.countries.get(alpha_2=code)
        if country:
            return {"code": code, "name": country.name}

    # Exact name match
    country = pycountry.countries.get(name=text)
    if country:
        return {"code": country.alpha_2, "name": country.name}

    # Fuzzy search
    try:
        results = pycountry.countries.search_fuzzy(text)
        if results:
            return {"code": results[0].alpha_2, "name": results[0].name}
    except LookupError:
        pass

    logger.warning("Could not resolve country: %s", country_text)
    return None


def resolve_nationalities(nationality_text: str | None) -> list[dict]:
    """Convert a nationality string to a BODS nationalities array.

    Args:
        nationality_text: e.g. "British", "American", "French, German"

    Returns:
        [{"code": "GB", "name": "United Kingdom"}] or [].
    """
    if not nationality_text or not nationality_text.strip():
        return []

    results = []
    # Handle comma-separated or semicolon-separated nationalities
    parts = re.split(r"[,;/]", nationality_text)

    for part in parts:
        part = part.strip().lower()
        if not part:
            continue

        # Check demonym mapping first
        if part in DEMONYM_TO_COUNTRY:
            code = DEMONYM_TO_COUNTRY[part]
            country = pycountry.countries.get(alpha_2=code)
            if country:
                results.append({"code": code, "name": country.name})
                continue

        # Try as country name
        resolved = resolve_country(part)
        if resolved:
            results.append(resolved)

    return results


def jurisdiction_to_country_code(oc_jurisdiction_code: str) -> str | None:
    """Extract the ISO alpha-2 country code from an OC jurisdiction code.

    'gb' → 'GB', 'us_de' → 'US', 'ca_bc' → 'CA'
    """
    if not oc_jurisdiction_code:
        return None

    code = oc_jurisdiction_code.strip().lower()
    if "_" in code:
        return code.split("_")[0].upper()
    return code.upper()
