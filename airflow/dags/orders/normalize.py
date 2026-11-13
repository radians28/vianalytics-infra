"""stg_* rows (text) -> one standard line-level frame, whatever the marketplace.

Port of vianalytics-svc/sku_consolidation/common/normalize.py and the money / timestamp
parsers of common/parsers.py. Differences from the offline version:
- timestamps are Asia/Jakarta-aware (the export's wall clock with the zone attached, never converted)
  and fall back to the formats staging writes for Excel date cells (sku.rules.FALLBACK_TS)
- the Shopee listing key is built exactly as sku/process.py builds m_platform_sku.merchant_sku
- a blank marketplace line id falls back to the generated <listing>:<n> key instead of NULL
"""
import logging

import numpy as np
import pandas as pd

from orders.city import extract_zip
from orders.config import PY_TS, STG_MAP
from sku.rules import FALLBACK_TS, JAKARTA, shopee_listing_key

log = logging.getLogger(__name__)

BLANK = ['', '-', 'nan', 'None']


def clean_text(s: pd.Series) -> pd.Series:
    """Collapse whitespace; blank, '-', 'nan' -> NA (vector form of sku.rules.clean_text)."""
    s = s.astype('string').str.replace(r'\s+', ' ', regex=True).str.strip()
    return s.mask(s.isin(BLANK))


def parse_ts(s: pd.Series, fmt: str) -> pd.Series:
    """Wall-clock WIB text -> Asia/Jakarta-aware datetime64 (NaT when unreadable)."""
    s = clean_text(s)
    out = pd.to_datetime(s, format=fmt, errors='coerce')
    for f in FALLBACK_TS:
        miss = out.isna() & s.notna()
        if not miss.any():
            break
        out = out.where(~miss, pd.to_datetime(s.where(miss), format=f, errors='coerce'))
    return out.dt.tz_localize(JAKARTA)


def parse_money(s: pd.Series, style: str) -> pd.Series:
    """Text money -> float. 'dot': 'Rp391.000', '-Rp39.100', '414.090'; 'plain': '175898.00'."""
    s = s.astype('string').str.strip()
    if style == 'dot':
        neg = s.str.contains('-', na=False)
        digits = s.str.replace(r'[^0-9]', '', regex=True)
        v = pd.to_numeric(digits.replace('', pd.NA), errors='coerce').astype('float64')
        return v.where(~neg, -v)
    s = s.str.replace(r'[^0-9.\-]', '', regex=True)
    return pd.to_numeric(s.replace('', pd.NA), errors='coerce').astype('float64')


def _col(df: pd.DataFrame, name: str | None) -> pd.Series:
    if name is None or name not in df.columns:
        return pd.Series(pd.NA, index=df.index, dtype='string')
    return df[name].astype('string')


def normalize(df: pd.DataFrame, channel_code: str) -> pd.DataFrame:
    m = STG_MAP[channel_code]
    fmt = PY_TS[channel_code]
    n = pd.DataFrame(index=df.index)
    n['order_no'] = clean_text(_col(df, m['order_no']))
    for key, src in (('created_at', 'created'), ('paid_at', 'paid'),
                     ('shipped_at', 'shipped'), ('completed_at', 'completed')):
        n[key] = parse_ts(_col(df, m[src]), fmt)
    n['status'] = clean_text(_col(df, m['status']))
    n['item_status'] = clean_text(_col(df, m['item_status']))
    n['seller_sku'] = clean_text(_col(df, m['seller_sku']))
    n['product_name'] = clean_text(_col(df, m['product_name']))
    n['variation'] = clean_text(_col(df, m['variation']))
    if m['platform_sku'] is None:       # Shopee: no platform item id in the export
        n['platform_sku'] = pd.Series(
            [shopee_listing_key(None if pd.isna(a) else a, None if pd.isna(b) else b)
             for a, b in zip(n['product_name'], n['variation'])], index=df.index, dtype='string')
    else:
        n['platform_sku'] = clean_text(_col(df, m['platform_sku']))

    def money(key):
        return parse_money(_col(df, m[key]), m['money'])

    if m['qty']:
        q = pd.to_numeric(_col(df, m['qty']), errors='coerce')
        if q.isna().any():
            log.warning('%s: %d lines with non-numeric quantity -> 0', channel_code, q.isna().sum())
        n['quantity'] = q.fillna(0).astype(int)
    else:
        n['quantity'] = 1                   # Lazada: one export row per unit
    ret = pd.to_numeric(_col(df, m['return_qty']), errors='coerce') if m['return_qty'] else 0
    n['return_quantity'] = pd.Series(ret, index=df.index).fillna(0).clip(lower=0).astype(int)
    n['unit_price'] = money('unit_price').abs()
    n['seller_discount'] = money('seller_discount').abs() if m['seller_discount'] else np.nan
    n['platform_discount'] = money('platform_discount').abs() if m['platform_discount'] else np.nan
    n['gmv_1'] = (n['unit_price'] * n['quantity']).round(2)      # gross line value, before discounts
    n['buyer'] = clean_text(_col(df, m['buyer']))
    n['warehouse'] = clean_text(_col(df, m['warehouse']))
    n['delivery_option'] = clean_text(_col(df, m['delivery']))
    n['payment_method'] = clean_text(_col(df, m['payment']))
    n['city'] = clean_text(_col(df, m['city']))
    n['province'] = clean_text(_col(df, m['province']))
    n['zip_code'] = pd.Series([extract_zip(None if pd.isna(v) else v, m['zip_style'])
                                for v in _col(df, m['zip'])], index=df.index, dtype='string')
    n['shipping_fee'] = money('shipping_fee') if m['shipping_fee'] else np.nan
    n['order_total'] = money('order_total')
    n['source_file'] = df['source_file']
    n['source_row_no'] = pd.to_numeric(df['source_row_no'], errors='coerce')

    # line key: marketplace line id, else <platform sku or seller sku>:<n-th occurrence in order>.
    # Built over every line of the upload (pending ones too) so a key never shifts when the
    # pending SKU pipeline loads the remaining lines later.
    base = n['platform_sku'].fillna(n['seller_sku']).fillna('NA')
    seq = base.groupby([n['order_no'], base], dropna=False, sort=False).cumcount() + 1
    generated = base + ':' + seq.astype(int).astype(str)
    if m['line_key']:
        n['line_key'] = clean_text(_col(df, m['line_key'])).fillna(generated)
    else:
        n['line_key'] = generated
    return n
