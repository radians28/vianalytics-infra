"""Marketplace city / province wording -> canonical kabupaten/kota (royal_canin.m_city with a city_code).

No database access here: the caller builds a `Reference` from the canonical rows (the m_city rows that
have a city_code, or vianalytics-svc/sku_consolidation/reference/ref_city.csv) and asks `match()`.
Used by the order load (orders.load.ensure_cities) and the one-off cleanup (airflow/scripts/city_cleanup.py).

Rules (user decisions 2026-11-13):
- names are compared on a compact key: upper case, letters and digits only ("Pematang Siantar" =
  "PEMATANGSIANTAR", "Bau-Bau" = "BAUBAU", "Bekasi+Regency" = "BEKASI" + KABUPATEN)
- the type comes from the wording: Kota / City / Kota Administrasi -> KOTA; Kab. / Kabupaten /
  Regency -> KABUPATEN
- a bare name that exists as both Kota and Kabupaten in the province (TikTok "Bekasi") -> the KOTA,
  method bare_name_kota (flagged)
- Papua: raw "Papua" / "Papua Barat" also match the cities moved to the 2022 provinces
"""
import re
from dataclasses import dataclass

KOTA, KAB = 'KOTA', 'KABUPATEN'

# Kemendagri province code per compact spelling seen in the marketplace files (besides the official names)
_PROVINCE_SPELLINGS = {
    '11': ['ACEH', 'DIACEH', 'NAD', 'NANGGROEACEHDARUSSALAM'],
    '12': ['NORTHSUMATRA', 'SUMUT'],
    '13': ['WESTSUMATRA', 'SUMBAR'],
    '14': ['RIAU', '廖内'],
    '15': ['JAMBI'],
    '16': ['SOUTHSUMATRA', 'SUMSEL', 'GÜNEYSUMATRA', 'สุมาตราใต้'],
    '17': ['BENGKULU'],
    '18': ['LAMPUNG'],
    '19': ['BANGKABELITUNG', 'BANGKABELITUNGISLANDS', 'BABEL'],
    '21': ['RIAUISLANDS', 'KEPRI', '廖内群岛'],
    '31': ['JAKARTA', 'DKIJAKARTA', 'JAKARTARAYA', 'DKI', 'SPECIALCAPITALREGIONOFJAKARTA', '雅加达', 'جاكارتا',
           '자카르타특별수도지역', 'ОСОБЫЙСТОЛИЧНЫЙОКРУГДЖАКАРТА', 'CAKARTAÖZELBAŞKENTBÖLGESI',
           'ĐẶCKHUTHỦĐÔJAKARTA', 'เขตนครหลวงพิเศษจาการ์ตา'],
    '32': ['WESTJAVA', 'JABAR', '서자와', '西ジャワ', '西爪哇'],
    '33': ['CENTRALJAVA', 'JATENG', '中爪哇', '中爪哇省'],
    '34': ['YOGYAKARTA', 'DIYOGYAKARTA', 'DIY', 'SPECIALREGIONOFYOGYAKARTA'],
    '35': ['EASTJAVA', 'JATIM', 'JAWAWÉTAN', 'JAVAORIENTAL', '東ジャワ', '东爪哇', 'ชวาตะวันออก'],
    '36': ['BANTEN', 'バンテン', '万丹'],
    '51': ['BALI', 'БАЛИ', '巴厘岛'],
    '52': ['WESTNUSATENGGARA', 'NTB'],
    '53': ['EASTNUSATENGGARA', 'NTT'],
    '61': ['WESTKALIMANTAN', 'KALBAR'],
    '62': ['CENTRALKALIMANTAN', 'KALTENG'],
    '63': ['SOUTHKALIMANTAN', 'KALSEL'],
    '64': ['EASTKALIMANTAN', 'KALTIM'],
    '65': ['NORTHKALIMANTAN', 'KALTARA'],
    '71': ['NORTHSULAWESI', 'SULUT'],
    '72': ['CENTRALSULAWESI', 'SULTENG'],
    '73': ['SOUTHSULAWESI', 'SULSEL'],
    '74': ['SOUTHEASTSULAWESI', 'SULTRA'],
    '75': ['GORONTALO'],
    '76': ['WESTSULAWESI', 'SULBAR'],
    '81': ['MALUKU'],
    '82': ['NORTHMALUKU', 'MALUT'],
    '91': ['PAPUA'],
    '92': ['WESTPAPUA'],
    '93': ['SOUTHPAPUA'],
    '94': ['CENTRALPAPUA'],
    '95': ['HIGHLANDPAPUA'],
    '96': ['SOUTHWESTPAPUA'],
}
# provinces split off in 2022: a raw province still written the old way also covers its children
PROVINCE_CHILDREN = {'91': ('91', '93', '94', '95'), '92': ('92', '96')}

# old, misspelled or alternative names of a kabupaten/kota -> current compact name (seen in the files)
RENAMED = {
    'TOBASAMOSIR': 'TOBA',
    'PANGKAJENEKEPULAUAN': 'PANGKAJENEDANKEPULAUAN',
    'MAMUJUUTARA': 'PASANGKAYU',
    'PAHUWATO': 'POHUWATO',
    'PADANGSIDEMPUAN': 'PADANGSIDIMPUAN',
    'LIMAPULUHKOTOKOTA': 'LIMAPULUHKOTA',
    'LIMAPULUHKOTO': 'LIMAPULUHKOTA',
    'SIDENRENGRAPPANGRAPANG': 'SIDENRENGRAPPANG',
    'SELAYAR': 'KEPULAUANSELAYAR',
    'PULAUTIDORE': 'TIDOREKEPULAUAN',
    'TIDORE': 'TIDOREKEPULAUAN',
    'SUNGAIKAPUAS': 'KAPUAS',
    'DEMPASSAR': 'DENPASAR',
    'BOLAANGMONGODOWTIMUR': 'BOLAANGMONGONDOWTIMUR',
    'JAKARTATENGAH': 'JAKARTAPUSAT',
    'LIMAPULUH': 'LIMAPULUHKOTA',
    'SURABAIA': 'SURABAYA',
}
# same name, but the old kabupaten was renamed while the kota kept the name
RENAMED_TYPED = {(KAB, 'PONTIANAK'): 'MEMPAWAH'}

# city names written in another script (TikTok shows the buyer's app language) -> Indonesian wording
FOREIGN_CITY = {
    '北雅加达': 'Kota Jakarta Utara', '中雅加达': 'Kota Jakarta Pusat', '西雅加达': 'Kota Jakarta Barat',
    '南雅加达': 'Kota Jakarta Selatan', '东雅加达': 'Kota Jakarta Timur',
    '唐格朗县': 'Kabupaten Tangerang', '唐格朗市': 'Kota Tangerang', '三宝垄市': 'Kota Semarang',
    'ブカシリージェンシー': 'Kabupaten Bekasi', '반둥': 'Bandung', '巴淡岛': 'Kota Batam',
    '加拉璜': 'Kabupaten Karawang', '克隆功': 'Kabupaten Klungkung',
}

# English direction word in front ("South Jakarta") -> Indonesian word behind ("Jakarta Selatan")
_DIRECTION = [('SOUTHEAST ', 'TENGGARA'), ('SOUTHWEST ', 'BARAT DAYA'), ('SOUTH ', 'SELATAN'),
              ('NORTH ', 'UTARA'), ('EAST ', 'TIMUR'), ('WEST ', 'BARAT'), ('CENTRAL ', 'TENGAH')]

_TYPE_PREFIX = [  # longest first
    ('KOTA ADMINISTRASI ', KOTA), ('KOTA ADM. ', KOTA), ('KOTA ADM ', KOTA),
    ('KABUPATEN ADMINISTRASI ', KAB), ('KAB. ADM. ', KAB), ('KAB ADM ', KAB),
    ('KABUPATEN ', KAB), ('KAB. ', KAB), ('KAB.', KAB), ('KAB ', KAB), ('KOTA ', KOTA), ('KUTHA ', KOTA),
]
_OFFICIAL_PREFIX = ('KOTA ADMINISTRASI ', 'KABUPATEN ADMINISTRASI ', 'KOTA ', 'KABUPATEN ')
_TYPE_SUFFIX = [(' REGENCY', KAB), (' CITY', KOTA), (' KABUPATEN', KAB), (' KAB.', KAB), (' KAB', KAB),
                (' KOTA', KOTA)]


def compact(text: str) -> str:
    """Upper case, letters and digits only (unicode letters kept)."""
    return ''.join(ch for ch in str(text).upper() if ch.isalnum())


def _clean(text) -> str:
    s = re.sub(r'\([^)]*\)', ' ', str(text or '').replace('+', ' '))   # drop "(NTT)" / "(Kota)"
    s = re.sub(r'\s+', ' ', s).strip().upper()
    return re.sub(r'(?<![A-Z])KEP\.\s*', 'KEPULAUAN ', s)          # reference: "Kep. Siau Tagulandang Biaro"


def normalise_city(raw) -> tuple[str | None, str]:
    """'Kab. Bekasi' -> ('KABUPATEN', 'BEKASI'); 'Bekasi City' -> ('KOTA', 'BEKASI'); 'Bekasi' -> (None, 'BEKASI').
    'Kabupaten Kota Bekasi' (both words) -> (None, 'BEKASI')."""
    s = _clean(FOREIGN_CITY.get(str(raw or '').strip(), raw))
    if s.startswith('KECAMATAN '):      # "Kecamatan Garut Kota": a district written as the city
        s = s[len('KECAMATAN '):]
    types = []
    changed = True
    while changed:                      # 'KABUPATEN KOTA BEKASI' -> both types seen
        changed = False
        for prefix, t in _TYPE_PREFIX:
            if s.startswith(prefix) and len(s) > len(prefix):
                s, changed = s[len(prefix):].strip(), True
                types.append(t)
                break
        for suffix, t in _TYPE_SUFFIX:
            if s.endswith(suffix) and len(s) > len(suffix):
                s, changed = s[:-len(suffix)].strip(), True
                types.append(t)
                break
    if s.endswith(' ISLANDS'):          # "Meranti Islands" -> Kepulauan Meranti
        s = 'KEPULAUAN ' + s[:-len(' ISLANDS')]
    for word, indo in _DIRECTION:
        if s.startswith(word) and len(s) > len(word):
            s = f'{s[len(word):]} {indo}'
            break
    kind = types[0] if len(set(types)) == 1 else None
    return kind, compact(s)


def _keep_kota_word(raw) -> tuple[str | None, str] | None:
    """'KAB. KOTA WARINGIN TIMUR' / 'KAB. KOTA BARU': the second word belongs to the name
    (Kotawaringin Timur, Kotabaru) -> ('KABUPATEN', 'KOTAWARINGINTIMUR')."""
    s = _clean(raw)
    for prefix, t in _TYPE_PREFIX:
        if s.startswith(prefix) and s[len(prefix):].startswith('KOTA '):
            return t, compact(s[len(prefix):])
    return None


@dataclass(frozen=True)
class RefCity:
    city_id: int | None
    city_code: str
    city_name: str
    city_type: str
    province_code: str
    province_name: str

    @property
    def key(self) -> str:
        """Official name without its one type prefix ("Kabupaten Lima Puluh Kota" -> LIMAPULUHKOTA)."""
        s = _clean(self.city_name)
        for prefix in _OFFICIAL_PREFIX:
            if s.startswith(prefix):
                return compact(s[len(prefix):])
        return compact(s)


@dataclass(frozen=True)
class Match:
    city: RefCity | None
    method: str              # name_exact | name_normalized | bare_name_kota | unresolved
    flag: str | None = None  # bare_name_kota, type_mismatch, province_unknown, province_mismatch


class Reference:
    def __init__(self, cities):
        self.cities = list(cities)
        self.by_key: dict[str, list[RefCity]] = {}
        self.province: dict[str, str] = {}
        for c in self.cities:
            self.by_key.setdefault(c.key, []).append(c)
            self.province[compact(c.province_name)] = c.province_code
        for code, spellings in _PROVINCE_SPELLINGS.items():
            for sp in spellings:
                self.province.setdefault(compact(sp), code)

    def province_code(self, raw) -> str | None:
        k = compact(_clean(raw))
        for suffix in ('PROVINCE', 'PROVINSI'):
            if k.endswith(suffix) and k[:-len(suffix)] in self.province:
                k = k[:-len(suffix)]
        for prefix in ('PROVINSI', 'PROV'):
            if k.startswith(prefix) and k[len(prefix):] in self.province:
                k = k[len(prefix):]
        return self.province.get(k)

    def match(self, raw_city, raw_province) -> Match:
        kind, key = normalise_city(raw_city)
        if not key:
            return Match(None, 'unresolved')
        key = RENAMED_TYPED.get((kind, key), key)
        cands = self.by_key.get(key) or self.by_key.get(RENAMED.get(key, ''), [])
        if not cands and (alt := _keep_kota_word(raw_city)) and alt[1] in self.by_key:
            kind, cands = alt[0], self.by_key[alt[1]]
        if not cands:
            return Match(None, 'unresolved')
        pcode = self.province_code(raw_province)
        flag = None
        if pcode:
            allowed = PROVINCE_CHILDREN.get(pcode, (pcode,))
            in_prov = [c for c in cands if c.province_code in allowed]
            if in_prov:
                cands = in_prov
            elif len({c.city_code for c in cands}) == 1:
                flag = 'province_mismatch'      # one city of that name in Indonesia, province written wrong
            else:
                return Match(None, 'unresolved', 'province_mismatch')
        else:
            flag = 'province_unknown'
        if kind:
            typed = [c for c in cands if c.city_type == kind]
            if len(typed) == 1:
                exact = _clean(raw_city) == _clean(typed[0].city_name)
                return Match(typed[0], 'name_exact' if exact and not flag else 'name_normalized', flag)
            if not typed and len(cands) == 1:   # "Kab. Batu": only a Kota Batu exists
                return Match(cands[0], 'name_normalized', _join(flag, 'type_mismatch'))
            if len(typed) > 1:
                return Match(None, 'unresolved', _join(flag, 'ambiguous_name'))
        if len(cands) == 1:
            return Match(cands[0], 'name_normalized', flag)
        kota = [c for c in cands if c.city_type == KOTA]
        if len(kota) == 1 and all(c.city_type == KAB for c in cands if c is not kota[0]) and len(cands) == 2:
            return Match(kota[0], 'bare_name_kota', _join(flag, 'bare_name_kota'))
        return Match(None, 'unresolved', _join(flag, 'ambiguous_name'))


def _join(*flags) -> str | None:
    return ', '.join(f for f in flags if f) or None


def pick_city(zip_city, zip_other_cities, wording_city):
    """City of one order (user decision 2026-11-13): the postal code decides, unless the reference lists
    that code in several kabupaten/kota and the order's wording names one of the others."""
    if zip_city is None:
        return wording_city
    if wording_city is not None and wording_city in (zip_other_cities or ()):
        return wording_city
    return zip_city


ZIP = re.compile(r'(?<!\d)(\d{5})(?!\d)')


def extract_zip(text, style: str) -> str | None:
    """'field': the whole value must be 5 digits · 'address': the last 5-digit group in the address."""
    if text is None:
        return None
    s = str(text).strip()
    if style == 'field':
        return s if re.fullmatch(r'\d{5}', s) else None
    found = ZIP.findall(s)
    return found[-1] if found else None
