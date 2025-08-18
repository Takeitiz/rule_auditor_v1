"""
Timezone mapping constants for rule auditor.
"""

# Core timezone mappings - single source of truth
_CORE_TIMEZONE_MAP = {
    'NY': 'America/New_York',
    'TK': 'Asia/Tokyo',
    'LN': 'Europe/London',
    'GMT': 'GMT'
}

# Region to timezone mappings
_REGION_TIMEZONE_MAP = {
    'AMR': 'America/New_York',  # Americas
    'ASI': 'Asia/Tokyo',  # Asia
    'AUS': 'Asia/Tokyo',  # Australia (using Tokyo for consistency)
    'CAN': 'America/New_York',  # Canada
    'CHN': 'Asia/Tokyo',  # China (using Tokyo for consistency)
    'EAS': 'Asia/Tokyo',  # East Asia
    'EUR': 'Europe/London',  # Europe
    'GLOBAL': 'GMT',  # Global
    'IND': 'Asia/Tokyo',  # India (using Tokyo for consistency)
    'JPN': 'Asia/Tokyo',  # Japan
    'MEA': 'Europe/London',  # Middle East/Africa (using London for consistency)
    'USA': 'America/New_York',  # USA
    'XJP': 'Asia/Tokyo'  # Ex-Japan
}

# Country to timezone code mappings
_COUNTRY_TIMEZONE_MAP = {
    # Americas -> NY (America/New_York)
    'US': 'NY', 'CA': 'NY', 'MX': 'NY', 'BR': 'NY', 'AR': 'NY',
    'CL': 'NY', 'CO': 'NY', 'PE': 'NY', 'VE': 'NY',

    # Europe/Middle East/Africa -> LN (Europe/London)
    'GB': 'LN', 'DE': 'LN', 'FR': 'LN', 'IT': 'LN', 'ES': 'LN',
    'NL': 'LN', 'BE': 'LN', 'CH': 'LN', 'AT': 'LN', 'SE': 'LN',
    'NO': 'LN', 'DK': 'LN', 'FI': 'LN', 'PL': 'LN', 'PT': 'LN',
    'GR': 'LN', 'CZ': 'LN', 'HU': 'LN', 'RO': 'LN', 'IE': 'LN',
    'TR': 'LN', 'ZA': 'LN', 'AE': 'LN', 'SA': 'LN', 'QA': 'LN',
    'KW': 'LN', 'EG': 'LN', 'IL': 'LN', 'RU': 'LN',

    # Asia/Pacific -> TK (Asia/Tokyo)
    'JP': 'TK', 'CN': 'TK', 'KR': 'TK', 'TW': 'TK', 'HK': 'TK',
    'SG': 'TK', 'MY': 'TK', 'TH': 'TK', 'VN': 'TK', 'ID': 'TK',
    'PH': 'TK', 'IN': 'TK', 'PK': 'TK', 'AU': 'TK', 'NZ': 'TK'
}

# Public API - maintain backward compatibility
TIMEZONE_MAP = _CORE_TIMEZONE_MAP.copy()
TIMEZONE_MAP_REVERSE = {v: k for k, v in TIMEZONE_MAP.items()}
REGION_TIMEZONE_MAP = _REGION_TIMEZONE_MAP.copy()
COUNTRY_TIMEZONE_MAP = _COUNTRY_TIMEZONE_MAP.copy()

# Deprecated - kept for backward compatibility, will be removed in future versions
REGION_TIMEZONE_MAP_KEEP = {
    'AMR': 'America/New_York',     # Americas
    'ASI': 'Asia/Singapore',       # Asia
    'AUS': 'Australia/Sydney',     # Australia
    'CAN': 'America/Toronto',      # Canada
    'CHN': 'Asia/Shanghai',        # China
    'EAS': 'Asia/Tokyo',           # East Asia
    'EUR': 'Europe/London',        # Europe
    'GLOBAL': 'GMT',               # Global
    'IND': 'Asia/Kolkata',         # India
    'JPN': 'Asia/Tokyo',           # Japan
    'MEA': 'Asia/Dubai',           # Middle East/Africa
    'USA': 'America/New_York',     # USA
    'XJP': 'Asia/Tokyo'            # Ex-Japan
}

# Utility functions for timezone operations
def get_timezone_for_region(region: str) -> str:
    """Get timezone for a given region code."""
    return _REGION_TIMEZONE_MAP.get(region)

def get_timezone_for_country(country: str) -> str:
    """Get timezone for a given country code."""
    timezone_code = _COUNTRY_TIMEZONE_MAP.get(country)
    if timezone_code:
        return _CORE_TIMEZONE_MAP.get(timezone_code)
    return None

def get_all_supported_timezones() -> list:
    """Get list of all supported timezone strings."""
    return list(set(_CORE_TIMEZONE_MAP.values()))
