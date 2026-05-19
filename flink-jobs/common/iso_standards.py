"""
ISO standards utilities for the rt-payments pipeline.

References:
  - ISO 8583  : Financial transaction card-originated messages
  - ISO 4217  : Currency codes
  - ISO 7812  : Identification of issuers (PAN structure + Luhn)
  - ISO 8601  : Date and time
  - ISO 18245 : Merchant Category Codes
  - PCI DSS   : PAN masking requirements
"""
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional


# ─── ISO 8583 Message Type Indicator (MTI) ─────────────────────────────────
ISO8583_MTI = {
    "1100": "Authorization Request",
    "1101": "Authorization Request Repeat",
    "1110": "Authorization Request Response",
    "1120": "Authorization Advice",
    "1121": "Authorization Advice Repeat",
    "1130": "Authorization Advice Response",
    "1200": "Financial Request",
    "1201": "Financial Request Repeat",
    "1210": "Financial Request Response",
    "1220": "Financial Advice",
    "1221": "Financial Advice Repeat",
    "1230": "Financial Advice Response",
    "1420": "Reversal Advice",
    "1421": "Reversal Advice Repeat",
    "1430": "Reversal Advice Response",
}


# ─── ISO 4217 Currency Codes (subset relevant to the dataset) ─────────────
# numeric → (alpha, name, minor_units)
ISO4217 = {
    "008": ("ALL", "Albanian Lek", 2),
    "012": ("DZD", "Algerian Dinar", 2),
    "036": ("AUD", "Australian Dollar", 2),
    "124": ("CAD", "Canadian Dollar", 2),
    "156": ("CNY", "Chinese Yuan", 2),
    "344": ("HKD", "Hong Kong Dollar", 2),
    "356": ("INR", "Indian Rupee", 2),
    "392": ("JPY", "Japanese Yen", 0),
    "410": ("KRW", "South Korean Won", 0),
    "504": ("MAD", "Moroccan Dirham", 2),
    "643": ("RUB", "Russian Ruble", 2),
    "682": ("SAR", "Saudi Riyal", 2),
    "702": ("SGD", "Singapore Dollar", 2),
    "710": ("ZAR", "South African Rand", 2),
    "752": ("SEK", "Swedish Krona", 2),
    "756": ("CHF", "Swiss Franc", 2),
    "784": ("AED", "UAE Dirham", 2),
    "788": ("TND", "Tunisian Dinar", 3),
    "826": ("GBP", "Pound Sterling", 2),
    "840": ("USD", "US Dollar", 2),
    "971": ("AFN", "Afghan Afghani", 2),
    "978": ("EUR", "Euro", 2),
    "986": ("BRL", "Brazilian Real", 2),
}


def is_valid_currency(code: str) -> bool:
    """ISO 4217 numeric code validation."""
    return code in ISO4217


def currency_alpha(code: str) -> Optional[str]:
    entry = ISO4217.get(code)
    return entry[0] if entry else None


def currency_minor_units(code: str) -> int:
    entry = ISO4217.get(code)
    return entry[2] if entry else 2


# ─── ISO 18245 Merchant Category Codes (subset present in dataset) ─────────
MCC = {
    "3504": "Hilton International (Lodging)",
    "3681": "Conrad Hotels (Lodging)",
    "4816": "Computer Network/Information Services",
    "5411": "Grocery Stores, Supermarkets",
    "5541": "Service Stations (with or without Ancillary Services)",
    "5812": "Eating Places, Restaurants",
    "5912": "Drug Stores and Pharmacies",
    "5932": "Antique Shops",
    "5947": "Gift, Card, Novelty, and Souvenir Shops",
    "5971": "Art Dealers and Galleries",
    "5993": "Cigar Stores and Stands",
    "5999": "Miscellaneous and Specialty Retail Stores",
    "6010": "Manual Cash Disbursements - Financial Institutions",
    "6011": "Automated Cash Disbursements (ATM)",
    "6012": "Financial Institutions — Merchandise, Services",
    "6051": "Non-Financial Institutions — Foreign Currency, Money Orders",
    "7011": "Lodging — Hotels, Motels, Resorts",
    "7311": "Advertising Services",
    "8999": "Professional Services (Not Elsewhere Classified)",
}


def mcc_description(mcc: str) -> Optional[str]:
    return MCC.get(mcc)


# ─── ISO 7812 — Luhn check (for PAN validation) ────────────────────────────
def luhn_check(pan: str) -> bool:
    """Mod-10 checksum per ISO/IEC 7812-1."""
    digits = [int(c) for c in pan if c.isdigit()]
    if len(digits) < 13 or len(digits) > 19:
        return False
    checksum = 0
    parity = len(digits) % 2
    for i, d in enumerate(digits):
        if i % 2 == parity:
            d *= 2
            if d > 9:
                d -= 9
        checksum += d
    return checksum % 10 == 0


def card_scheme(pan: str) -> Optional[str]:
    """Detect card scheme from BIN ranges."""
    if not pan or not pan[0].isdigit():
        return None
    if pan.startswith("4"):
        return "VISA"
    if pan[:2] in {"51", "52", "53", "54", "55"} or (len(pan) >= 4 and 2221 <= int(pan[:4]) <= 2720):
        return "MASTERCARD"
    if pan[:2] in {"34", "37"}:
        return "AMEX"
    if pan[:2] == "62":
        return "UNIONPAY"
    if pan.startswith("6011") or pan[:2] == "65":
        return "DISCOVER"
    return "UNKNOWN"


# ─── PCI DSS — PAN masking ─────────────────────────────────────────────────
def mask_pan(pan: str) -> str:
    """Per PCI DSS 3.4: display at most first 6 + last 4 digits."""
    if not pan or len(pan) < 10:
        return "*" * (len(pan) if pan else 0)
    return f"{pan[:6]}{'*' * (len(pan) - 10)}{pan[-4:]}"


def pan_bin(pan: str) -> Optional[str]:
    """ISO/IEC 7812 IIN — first 6 digits."""
    return pan[:6] if pan and len(pan) >= 6 else None


# ─── ISO 8601 — date/time normalization ────────────────────────────────────
_DATE_FORMATS = [
    "%d/%m/%Y %H:%M:%S",   # Powercard format: "16/09/2025 09:49:28"
    "%d/%m/%Y",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
]


def to_iso8601(value: Optional[str]) -> Optional[str]:
    """Parse a date/time in any of the known formats and return ISO 8601 UTC."""
    if not value:
        return None
    s = value.strip()
    if not s:
        return None
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
    return None


def business_date_from(value: Optional[str]) -> Optional[str]:
    """Extract YYYY-MM-DD partition key from a Powercard date."""
    iso = to_iso8601(value)
    return iso[:10] if iso else None
