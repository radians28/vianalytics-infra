"""SKU text rules shared by the SKU master steps.

Ported from vianalytics-svc/sku_consolidation (common/parsers.py, common/skutils.py,
match_bundle_master_to_stg.py) so the DAG keys and classifies SKUs exactly like the
offline pipeline that built the master tables.
"""
import hashlib
import re
from datetime import datetime
from zoneinfo import ZoneInfo

JAKARTA = ZoneInfo('Asia/Jakarta')

CHANNEL_PREFIX = re.compile(r'^(TKP|TTS|LZD|SHP|BLI)[_\-]', re.I)
MULTIPLIER = re.compile(r'^((?:\d+X)+)')
MULTIPACK_NAME = re.compile(r'^([2-9]|\d{2,})\s*x\s|(^|\s)([2-9]|\d{2,})\s*x\s*\d'
                            r'|\d\s*(g|gr|kg)\s*x\s*([2-9]|\d{2,})(?!\d)|set of \d+')
BLANK = {'', '-', 'nan', 'None'}

# Excel date cells reach staging as '%d/%m/%Y %H:%M:%S' (staging.common.cell_text),
# whatever the channel's own text format is
FALLBACK_TS = ('%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M')


def clean_text(value) -> str | None:
    """Collapse whitespace; blank, '-', 'nan' -> None."""
    if value is None:
        return None
    text = re.sub(r'\s+', ' ', str(value)).strip()
    return None if text in BLANK else text


def clean_sku(value) -> str | None:
    """R1: trim, upper-case, drop the channel prefix (TKP_, TTS_ ...), lower-case multiplier notation."""
    text = clean_text(value)
    if text is None:
        return None
    text = CHANNEL_PREFIX.sub('', text.upper())
    parts = [MULTIPLIER.sub(lambda m: m.group(1).lower(), p.strip()) for p in text.split('+')]
    return '+'.join(parts)


def shopee_listing_key(name: str | None, variation: str | None) -> str:
    """Shopee exports carry no platform item id: SHP_ + md5(name|variation)[:12]."""
    norm = (name or '').lower().strip() + '|' + (variation or '').lower().strip()
    return 'SHP_' + hashlib.md5(norm.encode('utf-8')).hexdigest()[:12]


def parse_ts(value, fmt: str) -> datetime | None:
    """Wall-clock WIB text from the export -> Asia/Jakarta aware datetime (None when unreadable)."""
    text = clean_text(value)
    if text is None:
        return None
    for f in (fmt, *FALLBACK_TS):
        try:
            return datetime.strptime(text, f).replace(tzinfo=JAKARTA)
        except ValueError:
            continue
    return None


def sku_type(code: str | None, name: str | None = '') -> str:
    """Classify from the SKU code; "37/..." codes carry no pattern, so fall back to the name."""
    name = str(name or '').lower()
    if code is None:
        return 'unknown'
    if '+' in code or code.startswith('BUND') or ' + ' in name:
        return 'bundle'
    if 'NEAREXPIRED' in code or 'expired' in name:
        return 'near_expired'
    if code.endswith('FG') or 'for gift' in name or 'not for sale' in name:
        return 'free_gift'
    if (MULTIPLIER.match(code.upper()) and not code.upper().startswith('1X')) or MULTIPACK_NAME.search(name):
        return 'multipack'
    return 'single'


def normalise_name(name) -> str:
    """Product name for similarity: lower-case, split digits from letters, symbols dropped."""
    s = str(name or '').lower()
    s = re.sub(r'(?<=\d),(?=\d)', '.', s)                       # 1,5 -> 1.5
    s = re.sub(r'(?<=\d)(?=[a-z])|(?<=[a-z])(?=\d)', ' ', s)    # 400g -> 400 g, x12 -> x 12
    s = re.sub(r'[^a-z0-9.\s]', ' ', s)                         # scrub symbols, keep spacing
    s = re.sub(r'(?<!\d)\.|\.(?!\d)', ' ', s)                   # '.' only inside decimals
    return re.sub(r'\s+', ' ', s).strip()
