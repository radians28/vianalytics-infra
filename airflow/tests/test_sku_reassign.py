"""SKU merge / split order line moves (airflow/dags/orders/reassign.py, rebundle.py), no database.

Run inside the Airflow image:
    docker exec -i vianalytics-airflow-scheduler-1 env PYTHONPATH=/opt/airflow/dags \
        python - < airflow/tests/test_sku_reassign.py
"""
import unittest

import pandas as pd

from orders.reassign import split_moves
from orders.rebundle import _originals, _plain


class FakeLookups:
    """code -> sku and listing -> sku as orders.rebundle.Lookups after the master change."""

    def __init__(self, code, listing):
        self.code, self.listing = code, listing

    def line_sku(self, seller_sku, platform_sku_id):
        from sku.rules import clean_sku
        code = clean_sku(seller_sku) if seller_sku else None
        sku = self.code.get(code) if code else None
        if sku is None and platform_sku_id is not None:
            sku = self.listing.get(int(platform_sku_id))
        return sku


def rows(*items):
    return pd.DataFrame([{'order_detail_id': i, 'seller_sku': s, 'platform_sku_id': p, 'channel_id': 2}
                         for i, (s, p) in enumerate(items, 1)], dtype=object)


class SplitMovesTest(unittest.TestCase):
    # source 341 kept code 37/020200201B and listing 900; code 20300010 and listing 901 moved to 342
    lookups = FakeLookups({'37/020200201B': 341, '20300010': 342}, {900: 341, 901: 342})

    def test_moved_seller_sku_moves_the_line(self):
        moved = split_moves(rows(('TKP_20300010', 900)), self.lookups, 342)
        self.assertEqual(list(moved['order_detail_id']), [1])

    def test_kept_seller_sku_stays_even_on_a_moved_listing(self):
        self.assertTrue(split_moves(rows(('37/020200201B', 901)), self.lookups, 342).empty)

    def test_unknown_seller_sku_follows_the_listing(self):
        moved = split_moves(rows(('OLD-CODE', 901), ('OLD-CODE', 900), (None, 901)), self.lookups, 342)
        self.assertEqual(list(moved['order_detail_id']), [1, 3])

    def test_no_rows(self):
        self.assertTrue(split_moves(rows(), self.lookups, 342).empty)


class PlainRebuildTest(unittest.TestCase):
    """A bundle listing split onto a SKU without components: its split rows become one plain line."""
    old_bom = [{'seq': 1, 'component_sku_id': 10, 'qty': 1, 'price_ratio': '0.25'},
               {'seq': 2, 'component_sku_id': 11, 'qty': 6, 'price_ratio': '0.75'}]

    def stored(self):
        base = {'order_id': 7, 'platform_sku_id': 901, 'seller_sku': 'BDL-1', 'product_name': 'Bundle',
                'item_status': 'Selesai', 'return_quantity': 0, 'is_bundle': True, 'channel_id': 2}
        return pd.DataFrame([
            {**base, 'order_detail_id': 1, 'line_key': 'L1#1', 'sku_id': 10, 'quantity': 2,
             'unit_price': 25000, 'seller_discount': 500, 'platform_discount': 0, 'gmv_1': 50000},
            {**base, 'order_detail_id': 2, 'line_key': 'L1#2', 'sku_id': 11, 'quantity': 12,
             'unit_price': 12500, 'seller_discount': 1500, 'platform_discount': 0, 'gmv_1': 150000},
        ], dtype=object)

    def test_rebuilt_line_is_plain_with_same_totals(self):
        lines, mismatch = _originals(self.stored(), 342, self.old_bom)
        self.assertEqual(mismatch, [])
        plain = _plain(lines)
        self.assertEqual(len(plain), 1)
        row = plain.iloc[0]
        self.assertEqual(row['line_key'], 'L1')
        self.assertEqual(row['sku_id'], 342)
        self.assertFalse(row['is_bundle'])
        self.assertEqual(row['quantity'], 2)                  # 2 bundles: 2 x 1 and 12 x 6 component units
        self.assertEqual(float(row['gmv_1']), 200000.0)
        self.assertEqual(float(row['seller_discount']), 2000.0)
        self.assertNotIn('base', plain.columns)
        self.assertIn('dq_flag', plain.columns)

    def test_groups_that_dont_add_up_are_left_alone(self):
        bad = self.stored()
        bad.loc[1, 'quantity'] = 13                           # 13 is not 2 x 6
        lines, mismatch = _originals(bad, 342, self.old_bom)
        self.assertEqual(sorted(mismatch), [1, 2])
        self.assertTrue(lines.empty or (lines['line_key'] != 'L1').all())


class BeforeAfterTest(unittest.TestCase):
    """Persian case: listing 901 moves from 341 (PCS) to 342 (BOX); code 401B already belonged to 342."""

    def test_only_rows_that_changed_owner_are_rebuilt(self):
        from orders.reassign import BeforeLookups
        after = FakeLookups({'37/020200201B': 341, '37/020200401B': 342, '20300010': 342}, {901: 342})
        snapshot = {'m_sku': [{'sku_id': 341}, {'sku_id': 342}],
                    'm_sku_alias': [{'sku_code': '37/020200201B', 'sku_id': 341},
                                    {'sku_code': '37/020200401B', 'sku_id': 342},
                                    {'sku_code': '20300010', 'sku_id': 342}],
                    'm_platform_sku': [{'platform_sku_id': 901, 'sku_id': 341}],
                    'm_sku_history': []}
        before = BeforeLookups(after, snapshot)
        cases = {('37/020200401B', 901): False,     # BOX code: 342 before and after
                 ('TKP_20300010', 901): False,      # BOX legacy code: 342 before and after
                 ('37/020200201B', 901): False,     # PCS code stays on 341
                 ('UNKNOWN-1', 901): True,          # follows the listing: 341 -> 342
                 (None, 901): True}
        for (seller, listing), moves in cases.items():
            changed = before.line_sku(seller, listing) == 341 and after.line_sku(seller, listing) == 342
            self.assertEqual(changed, moves, seller)


if __name__ == '__main__':
    unittest.main(verbosity=2)
