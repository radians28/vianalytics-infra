"""Product-name similarity between an incoming listing and the SKU master.

1. n-gram score, same method as sku_consolidation/match_bundle_master_to_stg.py: normalised
   names -> TF-IDF over character n-grams (3..5, word bounded) -> cosine similarity.
2. The best POOL names by cosine are re-scored with the product attributes of both names
   (sku/attributes.py: species, life stage, size, pack qty): score = cosine x multiplier, where
   each category contributes 1.0 (match), 0.95 (one side lacks it), 0.9 (both lack it) or
   0.7 (both have it, no shared value). So a same-looking name for another size, species or
   life stage drops below the real match.
A candidate is a close match when the adjusted score is >= 0.55 (distance <= MATCH_DISTANCE).
"""
from dataclasses import dataclass, field

from sku.attributes import extract, multiplier
from sku.rules import normalise_name

MATCH_DISTANCE = 0.45
TOP_N = 3
POOL = 50          # names re-scored with attributes, taken by cosine


@dataclass
class Candidate:
    sku_id: int
    sku_master_code: str
    name: str
    score: float                     # cosine x attribute multiplier
    cosine: float | None = None      # n-gram score alone
    factors: dict = field(default_factory=dict)

    def as_json(self) -> dict:
        return {'sku_id': self.sku_id, 'sku_master_code': self.sku_master_code,
                'name': self.name, 'score': self.score, 'cosine': self.cosine,
                'factors': self.factors}


class NameMatcher:
    """Names of the master SKUs and of their marketplace listings, one vector per (sku, name)."""

    def __init__(self, names: list[tuple[int, str, str]]):
        """names: (sku_id, sku_master_code, product name) - several names per SKU are fine."""
        from sklearn.feature_extraction.text import TfidfVectorizer

        seen, rows = set(), []
        for sku_id, code, name in names:
            norm = normalise_name(name)
            if norm and (sku_id, norm) not in seen:
                seen.add((sku_id, norm))
                rows.append((sku_id, code, name, norm, extract(name)))
        self.rows = rows
        self.vectorizer = None
        if rows:
            self.vectorizer = TfidfVectorizer(analyzer='char_wb', ngram_range=(3, 5), sublinear_tf=True)
            self.matrix = self.vectorizer.fit_transform([r[3] for r in rows])

    def top(self, name: str | None, n: int = TOP_N) -> list[Candidate]:
        """Best-scoring distinct SKUs for `name` by adjusted score, highest first."""
        norm = normalise_name(name)
        if not norm or self.vectorizer is None:
            return []
        attrs = extract(name)
        cosines = (self.matrix @ self.vectorizer.transform([norm]).T).toarray().ravel()
        scored = []
        for i in cosines.argsort()[::-1][:POOL]:
            cosine = float(min(cosines[i], 1.0))
            if cosine <= 0:
                break
            mult, factors = multiplier(attrs, self.rows[i][4])
            scored.append((cosine * mult, cosine, factors, i))
        scored.sort(key=lambda s: s[0], reverse=True)

        best: dict[int, Candidate] = {}
        for score, cosine, factors, i in scored:
            sku_id, code, label, _, _ = self.rows[i]
            if sku_id not in best:
                best[sku_id] = Candidate(sku_id, code, label, round(score, 4), round(cosine, 4), factors)
                if len(best) == n:
                    break
        return list(best.values())


def is_close(candidate: Candidate | None) -> bool:
    return candidate is not None and 1.0 - candidate.score <= MATCH_DISTANCE
