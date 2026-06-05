"""
ISO standards reference data and helpers for HPS pipeline validation/enrichment.

Covers:
  ISO 8583  — Message Type Indicators (MTI)
  ISO 4217  — Currency Codes
  ISO 18245 — Merchant Category Codes (MCC)
  ISO 7812  — Card number validation (Luhn algorithm)
"""

# ── ISO 8583 — Message Type Indicators ──────────────────────────────────────
ISO8583_MTI = {
    "1100": "Authorization Request",
    "1110": "Authorization Response",
    "1120": "Authorization Advice",
    "1130": "Authorization Advice Response",
    "1200": "Financial Request",
    "1210": "Financial Response",
    "1220": "Financial Advice",
    "1230": "Financial Advice Response",
    "1300": "File Action Request",
    "1310": "File Action Response",
    "1320": "File Action Advice",
    "1330": "File Action Advice Response",
    "1420": "Reversal Request",
    "1430": "Reversal Response",
    "1800": "Network Management Request",
}

# ── ISO 4217 — Currency Codes ────────────────────────────────────────────────
ISO4217 = {
    "504": {"alpha": "MAD", "minorUnits": 2},
    "840": {"alpha": "USD", "minorUnits": 2},
    "978": {"alpha": "EUR", "minorUnits": 2},
    "826": {"alpha": "GBP", "minorUnits": 2},
    "756": {"alpha": "CHF", "minorUnits": 2},
    "392": {"alpha": "JPY", "minorUnits": 0},
    "124": {"alpha": "CAD", "minorUnits": 2},
    "036": {"alpha": "AUD", "minorUnits": 2},
    "784": {"alpha": "AED", "minorUnits": 2},
    "682": {"alpha": "SAR", "minorUnits": 2},
    "634": {"alpha": "QAR", "minorUnits": 2},
    "414": {"alpha": "KWD", "minorUnits": 3},
    "788": {"alpha": "TND", "minorUnits": 3},
    "012": {"alpha": "DZD", "minorUnits": 2},
    "818": {"alpha": "EGP", "minorUnits": 2},
    "566": {"alpha": "NGN", "minorUnits": 2},
    "710": {"alpha": "ZAR", "minorUnits": 2},
    "356": {"alpha": "INR", "minorUnits": 2},
    "156": {"alpha": "CNY", "minorUnits": 2},
    "702": {"alpha": "SGD", "minorUnits": 2},
    "344": {"alpha": "HKD", "minorUnits": 2},
    "578": {"alpha": "NOK", "minorUnits": 2},
    "208": {"alpha": "DKK", "minorUnits": 2},
}

# ── ISO 18245 — Merchant Category Codes ─────────────────────────────────────
MCC_DESCRIPTIONS = {
    "5411": "Grocery Stores, Supermarkets",
    "5812": "Eating Places, Restaurants",
    "5541": "Service Stations",
    "4111": "Transportation – Suburban and Local Commuter",
    "4121": "Taxicabs / Limousines",
    "7011": "Hotels, Motels and Resorts",
    "5912": "Drug Stores and Pharmacies",
    "5999": "Miscellaneous and Specialty Retail Stores",
    "6011": "Automated Cash Disbursements",
    "6012": "Merchandise and Services – Financial Institution",
    "5311": "Department Stores",
    "5661": "Shoe Stores",
    "5732": "Electronics Stores",
    "5045": "Computers, Computer Peripherals Equipment",
    "7832": "Motion Picture Theaters",
    "8099": "Health Practitioners, Medical Services",
    "8299": "Schools, Other Educational Services",
    "4814": "Telecommunication Services",
}


def luhn_check(pan: str) -> bool:
    digits = [int(d) for d in pan if d.isdigit()]
    if len(digits) < 12:
        return False
    total = 0
    for i, d in enumerate(reversed(digits)):
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def is_valid_currency(code: str) -> bool:
    return str(code).strip() in ISO4217


def is_valid_mti(mti: str) -> bool:
    return str(mti).strip() in ISO8583_MTI


def currency_alpha(code: str) -> str | None:
    return ISO4217.get(str(code), {}).get("alpha")


def currency_minor_units(code: str) -> int | None:
    return ISO4217.get(str(code), {}).get("minorUnits")


def mcc_description(mcc: str) -> str | None:
    return MCC_DESCRIPTIONS.get(str(mcc).strip())


def card_scheme(pan: str) -> str:
    pan = pan.replace("*", "").strip()
    if pan.startswith("4"):
        return "VISA"
    if pan[:2] in [str(i) for i in range(51, 56)]:
        return "MASTERCARD"
    if pan[:2] in ["34", "37"]:
        return "AMEX"
    if pan.startswith("62"):
        return "UNIONPAY"
    if pan.startswith("6011") or pan[:2] == "65":
        return "DISCOVER"
    return "UNKNOWN"


def mask_pan(pan: str) -> str:
    digits = pan.replace("*", "").strip()
    if len(digits) < 10:
        return pan
    return digits[:6] + "*" * (len(digits) - 10) + digits[-4:]
