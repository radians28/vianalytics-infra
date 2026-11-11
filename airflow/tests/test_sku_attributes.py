"""Attribute-weighted SKU name matching (airflow/dags/sku/attributes.py, matcher.py).

Run inside the Airflow image, where scikit-learn is installed and the DAGs are mounted:
    docker exec -i vianalytics-airflow-scheduler-1 env PYTHONPATH=/opt/airflow/dags \
        python - < airflow/tests/test_sku_attributes.py
or with pytest / `python -m unittest` and PYTHONPATH=airflow/dags.
"""
import unittest

from sku.attributes import extract, factor, multiplier


def g(*grams):
    return frozenset(('g', x) for x in grams)


class ExtractTest(unittest.TestCase):
    def test_count_and_weight_in_either_order(self):
        a = extract('FHN INSTINCTIVE 12X85G')
        b = extract('Instinctive Wet 85gr x 12')
        c = extract('Instinctive (85 g x 12 pouches)')
        for x in (a, b, c):
            self.assertEqual(x.size, g(85))
            self.assertEqual(x.qty, frozenset({12}))

    def test_units_are_normalised(self):
        self.assertEqual(extract('Puppy 1,5kg').size, g(1500))
        self.assertEqual(extract('FHN KITTEN 2K (6)').size, g(2000))
        self.assertEqual(extract('Bengal Adult (0.4kg)').size, g(400))
        self.assertEqual(extract('Shampoo 250 cc').size, frozenset({('ml', 250)}))
        self.assertEqual(extract('Tin 1 L').size, frozenset({('ml', 1000)}))

    def test_master_carton_count_is_not_a_quantity(self):
        self.assertEqual(extract('FHN KITTEN 2K (6)').qty, frozenset())
        self.assertEqual(extract('FHN WET BABYCAT INSTNC 24X100G (24) - PCS').qty, frozenset({24}))

    def test_bilingual_species_and_stage(self):
        a = extract('Royal Canin Kitten (2kg) Dry Makanan Anak Kucing')
        self.assertEqual(a.species, frozenset({'cat'}))
        self.assertEqual(a.stage, frozenset({'young'}))
        b = extract('Royal Canin Mini Indoor Adult (3kg) Dry Makanan Anjing Dewasa')
        self.assertEqual(b.species, frozenset({'dog'}))
        self.assertEqual(b.stage, frozenset({'adult'}))

    def test_line_codes(self):
        self.assertEqual(extract('FHN EXIGENT 400G').species, frozenset({'cat'}))
        self.assertEqual(extract('SHN MINI ADULT 2KG').species, frozenset({'dog'}))

    def test_adult_plus_age_is_senior(self):
        self.assertEqual(extract('SHN MINI ADULT+8 2KG (6)').stage, frozenset({'senior'}))
        self.assertEqual(extract('Mini Adult 8+ 2kg').stage, frozenset({'senior'}))

    def test_bundle_keeps_every_value(self):
        a = extract('BDL - FBN KITTEN 2K + FBN PERSIAN ADULT 400G')
        self.assertEqual(a.size, g(2000, 400))
        self.assertEqual(a.stage, frozenset({'young', 'adult'}))

    def test_one_box_is_not_a_count(self):
        self.assertEqual(extract('Persian Wet 12x85gr - 1 Box').qty, frozenset({12}))


class FactorTest(unittest.TestCase):
    def test_table(self):
        self.assertEqual(factor(frozenset({'cat'}), frozenset({'cat'})), 1.0)
        self.assertEqual(factor(frozenset({'cat'}), frozenset()), 0.95)
        self.assertEqual(factor(frozenset(), frozenset({'dog'})), 0.95)
        self.assertEqual(factor(frozenset(), frozenset()), 0.9)
        self.assertEqual(factor(frozenset({'cat'}), frozenset({'dog'})), 0.7)

    def test_mixed_bundle_matches_on_any_shared_value(self):
        self.assertEqual(factor(frozenset({'adult', 'young'}), frozenset({'young'})), 1.0)

    def test_missing_categories_compound(self):
        total, factors = multiplier(extract('Royal Canin Persian'), extract('Royal Canin Persian'))
        # species cat? no keyword -> all four missing on both sides
        self.assertEqual(set(factors.values()), {0.9})
        self.assertAlmostEqual(total, 0.9 ** 4)

    def test_all_match(self):
        total, _ = multiplier(extract('FHN KITTEN 12X85G'), extract('Kitten Makanan Anak Kucing 85gr x 12'))
        self.assertEqual(total, 1.0)


class MatcherTest(unittest.TestCase):
    def test_matching_size_outranks_a_closer_looking_name(self):
        from sku.matcher import NameMatcher
        # same wording but 4kg vs reworded but 10kg: the size decides
        m = NameMatcher([
            (1, 'SKU-00001', 'Royal Canin Maxi Adult (4kg) Dry Makanan Anjing Dewasa - Size Health Nutrition'),
            (2, 'SKU-00002', 'Royal Canin Maxi Adult Makanan Anjing Dewasa Dry 10kg'),
        ])
        top = m.top('Royal Canin Maxi Adult (10kg) Dry Makanan Anjing Dewasa - Size Health Nutrition')
        self.assertEqual(top[0].sku_id, 2)
        self.assertGreater(next(c for c in top if c.sku_id == 1).cosine, top[0].cosine)
        self.assertEqual(top[0].factors['size'], 1.0)
        wrong = next(c for c in top if c.sku_id == 1)
        self.assertEqual(wrong.factors['size'], 0.7)
        self.assertLess(wrong.score, wrong.cosine)

    def test_candidate_json_carries_breakdown(self):
        from sku.matcher import NameMatcher
        m = NameMatcher([(1, 'SKU-00001', 'FHN KITTEN 2K (6)')])
        c = m.top('Royal Canin Kitten (2kg) Dry Makanan Anak Kucing')[0]
        j = c.as_json()
        self.assertEqual(set(j), {'sku_id', 'sku_master_code', 'name', 'score', 'cosine', 'factors'})
        self.assertEqual(j['factors'], {'species': 1.0, 'stage': 1.0, 'size': 1.0, 'qty': 0.9})
        self.assertAlmostEqual(j['score'], round(j['cosine'] * 0.9, 4), places=3)


if __name__ == '__main__':
    unittest.main(verbosity=2)
