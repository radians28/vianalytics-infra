"""Product-name similarity between an incoming listing and the SKU master.

Same method as sku_consolidation/match_bundle_master_to_stg.py: normalised names ->
TF-IDF over character n-grams (3..5, word bounded) -> cosine similarity. A candidate
is a close match when its cosine distance is within MATCH_DISTANCE (score >= 0.55).
"""
from dataclasses import dataclass

from sku.rules import normalise_name

MATCH_DISTANCE = 0.45
TOP_N = 3


@dataclass
class Candidate:
    sku_id: int
    sku_master_code: str
    name: str
    score: float

    def as_json(self) -> dict:
        return {'sku_id': self.sku_id, 'sku_master_code': self.sku_master_code,
                'name': self.name, 'score': self.score}


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
                rows.append((sku_id, code, name, norm))
        self.rows = rows
        self.vectorizer = None
        if rows:
            self.vectorizer = TfidfVectorizer(analyzer='char_wb', ngram_range=(3, 5), sublinear_tf=True)
            self.matrix = self.vectorizer.fit_transform([r[3] for r in rows])

    def top(self, name: str | None, n: int = TOP_N) -> list[Candidate]:
        """Best-scoring distinct SKUs for `name`, highest first."""
        norm = normalise_name(name)
        if not norm or self.vectorizer is None:
            return []
        scores = (self.matrix @ self.vectorizer.transform([norm]).T).toarray().ravel()
        best: dict[int, Candidate] = {}
        for i in scores.argsort()[::-1]:
            sku_id, code, label, _ = self.rows[i]
            if sku_id not in best:
                best[sku_id] = Candidate(sku_id, code, label, round(float(min(scores[i], 1.0)), 4))
                if len(best) == n:
                    break
        return list(best.values())


def is_close(candidate: Candidate | None) -> bool:
    return candidate is not None and 1.0 - candidate.score <= MATCH_DISTANCE
