"""rematch decisions (airflow/dags/sku/rematch.py decide), no database needed.

Run inside the Airflow image:
    docker exec -i vianalytics-airflow-scheduler-1 env PYTHONPATH=/opt/airflow/dags \
        python - < airflow/tests/test_sku_rematch.py
"""
import unittest

from sku.matcher import NameMatcher
from sku.process import Listing
from sku.rematch import CONFLICT, CREATE, MAP, MAP_NEW_CODE, MAP_NEW_LISTING, REFRESH, decide

CH = 4


class FakeMaster:
    def __init__(self, alias=None, platform=None, names=None):
        self.alias = alias or {}
        self.platform = {(CH, ms): {'sku_id': sku_id} for ms, sku_id in (platform or {}).items()}
        self._matcher = NameMatcher(names or [])

    def sku_of_code(self, code):
        return self.alias.get(code) if code else None

    def matcher(self):
        return self._matcher


def listing(merchant='L-1', code='C-1', name='Royal Canin Kitten (2kg) Dry Makanan Anak Kucing'):
    return Listing(merchant_sku=merchant, sku_code=code, seller_sku=code, product_name=name, variation=None)


class DecideTest(unittest.TestCase):
    def test_seller_sku_now_known(self):
        d = decide(FakeMaster(alias={'C-1': 7}), CH, listing())
        self.assertEqual((d.action, d.sku_id), (MAP_NEW_LISTING, 7))

    def test_listing_now_known_with_new_seller_sku(self):
        d = decide(FakeMaster(platform={'L-1': 9}), CH, listing())
        self.assertEqual((d.action, d.sku_id), (MAP_NEW_CODE, 9))

    def test_listing_known_without_seller_sku(self):
        d = decide(FakeMaster(platform={'L-1': 9}), CH, listing(code=None))
        self.assertEqual((d.action, d.sku_id), (MAP, 9))

    def test_both_known_same_sku(self):
        d = decide(FakeMaster(alias={'C-1': 9}, platform={'L-1': 9}), CH, listing())
        self.assertEqual((d.action, d.sku_id), (MAP, 9))

    def test_both_known_different_skus_is_a_conflict(self):
        d = decide(FakeMaster(alias={'C-1': 7}, platform={'L-1': 9}), CH, listing())
        self.assertEqual(d.action, CONFLICT)

    def test_other_channel_listing_does_not_count(self):
        m = FakeMaster(names=[(1, 'SKU-00001', 'FHN KITTEN 2K (6)')])
        m.platform[(CH + 1, 'L-1')] = {'sku_id': 1}
        self.assertIn(decide(m, CH, listing()).action, (REFRESH, CREATE))

    def test_close_name_refreshes_candidates(self):
        m = FakeMaster(names=[(1, 'SKU-00001', 'Royal Canin Kitten (2kg) Dry Makanan Anak Kucing - Feline Health')])
        d = decide(m, CH, listing())
        self.assertEqual(d.action, REFRESH)
        self.assertEqual(d.candidates[0].sku_id, 1)

    def test_no_close_name_creates(self):
        m = FakeMaster(names=[(1, 'SKU-00001', 'SHN MAXI ADULT 15KG (1)')])
        d = decide(m, CH, listing())
        self.assertEqual(d.action, CREATE)

    def test_empty_master_creates(self):
        self.assertEqual(decide(FakeMaster(), CH, listing()).action, CREATE)


if __name__ == '__main__':
    unittest.main(verbosity=2)
