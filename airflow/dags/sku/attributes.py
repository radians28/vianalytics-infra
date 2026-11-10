"""Product attributes read from a pet-food product name, used to weight the n-gram name score.

Four categories, each a set because a bundle name can carry several values:
- species: cat / dog
- stage:   young / adult / senior
- size:    weight in grams ('g', 2000) or volume in ml ('ml', 1000)
- qty:     pack count, e.g. 12 in "12x85gr", "85g x 12", "isi 12", "12 pcs"

Keywords are bilingual (English / Indonesian) and include Royal Canin line codes used in
the master names (FHN = Feline Health Nutrition, SHN = Size Health Nutrition, ...).

factor() compares one category of two names:
  both have it and share a value -> 1.0   (a bundle matches when any value is shared)
  only one side has it           -> 0.95
  neither side has it            -> 0.9
  both have it, no shared value  -> 0.7
multiplier() is the product over the four categories, so missing categories compound.
"""
import re
from dataclasses import dataclass

CATEGORIES = ('species', 'stage', 'size', 'qty')

FACTOR_MATCH = 1.0
FACTOR_ONE_MISSING = 0.95
FACTOR_BOTH_MISSING = 0.9
FACTOR_MISMATCH = 0.7

# whole words of the cleaned name -> value
SPECIES_WORDS = {
    'cat': ('cat', 'cats', 'kucing', 'kitten', 'kittens', 'feline', 'babycat', 'kitty',
            'fhn', 'fbn', 'fcn'),
    'dog': ('dog', 'dogs', 'anjing', 'puppy', 'puppies', 'canine', 'babydog',
            'shn', 'bhn', 'ccn'),
}
STAGE_WORDS = {
    'young': ('kitten', 'kittens', 'kit', 'puppy', 'puppies', 'junior', 'baby', 'babycat',
              'babydog', 'starter', 'anak'),
    'adult': ('adult', 'dewasa'),
    'senior': ('senior', 'ageing', 'aging', 'mature', 'tua', 'lansia'),
}
# "adult+8", "adult 8+", "mature 7+" are senior formulas, not adult ones
SENIOR_RE = re.compile(r'\b(?:adult|mature|ageing|aging)\s*(?:\+\s*\d+|\d+\s*\+)')

_UNITS = {
    'g': ('g', 1), 'gr': ('g', 1), 'grm': ('g', 1), 'gram': ('g', 1), 'grams': ('g', 1),
    'k': ('g', 1000), 'kg': ('g', 1000), 'kgs': ('g', 1000), 'kilo': ('g', 1000),
    'ml': ('ml', 1), 'cc': ('ml', 1),
    'l': ('ml', 1000), 'lt': ('ml', 1000), 'ltr': ('ml', 1000), 'liter': ('ml', 1000), 'litre': ('ml', 1000),
}
_UNIT = '|'.join(sorted(_UNITS, key=len, reverse=True))
_NUM = r'\d+(?:\.\d+)?'
SIZE = rf'({_NUM})\s*({_UNIT})(?![a-z])'
SIZE_RE = re.compile(SIZE)
# 12x85g / 12 x 85 gr   and   85g x 12 / 85g x 6 pouches; the count is not itself a size
COUNT_SIZE_RE = re.compile(rf'(?<![\d.])(\d+)\s*[x×*]\s*{SIZE}')
SIZE_COUNT_RE = re.compile(rf'{SIZE}\s*[x×*]\s*(\d+)(?![\d.])(?!\s*(?:{_UNIT})(?![a-z]))')
PACK_RE = re.compile(r'\bisi\s*(\d+)\b|(?<![\d.])(\d+)\s*(?:pcs|pc|pouch|pouches|sachet|sachets|kaleng|can|cans|box|boxes)\b')
# master names end with the carton count and unit: "FHN KITTEN 2K (6)", "... (24) - PCS"
CARTON_RE = re.compile(r'\(\s*\d+\s*\)\s*(?:-\s*(?:pcs|box)\s*)?$')


@dataclass(frozen=True)
class Attrs:
    species: frozenset
    stage: frozenset
    size: frozenset
    qty: frozenset


EMPTY = Attrs(frozenset(), frozenset(), frozenset(), frozenset())


def _clean(name) -> str:
    s = str(name or '').lower()
    s = re.sub(r'(?<=\d),(?=\d)', '.', s)            # 1,5 kg -> 1.5 kg
    return CARTON_RE.sub(' ', s.strip())


def _size(amount: str, unit: str) -> tuple:
    kind, factor = _UNITS[unit]
    value = round(float(amount) * factor, 3)
    return kind, int(value) if value == int(value) else value


def extract(name) -> Attrs:
    s = _clean(name)
    if not s:
        return EMPTY

    sizes, qtys = set(), set()

    def count_size(m):
        qtys.add(int(m.group(1)))
        sizes.add(_size(m.group(2), m.group(3)))
        return ' '

    def size_count(m):
        sizes.add(_size(m.group(1), m.group(2)))
        qtys.add(int(m.group(3)))
        return ' '

    def single(m):
        sizes.add(_size(m.group(1), m.group(2)))
        return ' '

    def pack(m):
        n = int(m.group(1) or m.group(2))
        if n > 1:                                     # "1 box" of 12x85g is one pack, not a count
            qtys.add(n)
        return ' '

    s = COUNT_SIZE_RE.sub(count_size, s)
    s = SIZE_COUNT_RE.sub(size_count, s)
    s = SIZE_RE.sub(single, s)
    s = PACK_RE.sub(pack, s)

    stages = set()
    if SENIOR_RE.search(s):
        stages.add('senior')
        s = SENIOR_RE.sub(' ', s)
    words = set(re.findall(r'[a-z]+', s))
    species = {k for k, kws in SPECIES_WORDS.items() if words.intersection(kws)}
    stages |= {k for k, kws in STAGE_WORDS.items() if words.intersection(kws)}
    return Attrs(frozenset(species), frozenset(stages), frozenset(sizes), frozenset(qtys))


def factor(a: frozenset, b: frozenset) -> float:
    if a and b:
        return FACTOR_MATCH if a & b else FACTOR_MISMATCH
    if a or b:
        return FACTOR_ONE_MISSING
    return FACTOR_BOTH_MISSING


def multiplier(a: Attrs, b: Attrs) -> tuple[float, dict]:
    """(product of the category factors, factor per category)."""
    factors = {c: factor(getattr(a, c), getattr(b, c)) for c in CATEGORIES}
    total = 1.0
    for f in factors.values():
        total *= f
    return total, factors
