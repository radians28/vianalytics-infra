"""City wording -> canonical kabupaten/kota (airflow/dags/orders/city.py), no database.

Run from the repo root:
    PYTHONPATH=airflow/dags python -m unittest airflow/tests/test_city.py
or inside the Airflow image:
    docker exec -i vianalytics-airflow-scheduler-1 env PYTHONPATH=/opt/airflow/dags \\
        python - < airflow/tests/test_city.py   (needs the reference CSV mounted; skipped otherwise)
"""
import csv
import unittest
from pathlib import Path

from orders.city import Reference, RefCity, extract_zip, normalise_city, pick_city

REF = Path(__file__).resolve().parents[2] / 'vianalytics-svc' / 'sku_consolidation' / 'reference' / 'ref_city.csv'


def load_ref():
    with open(REF, encoding='utf-8') as f:
        return Reference([RefCity(None, r['city_code'], r['city_name'], r['city_type'], r['province_code'],
                                  r['province_name']) for r in csv.DictReader(f)])


@unittest.skipUnless(REF.exists(), 'reference CSV not available')
class MatchTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ref = load_ref()

    def code(self, city, province):
        m = self.ref.match(city, province)
        return (m.city.city_code if m.city else None), m.method, m.flag

    def test_bekasi_variants(self):
        kota = ['KOTA BEKASI', 'Kota Bekasi', 'Bekasi City', 'Kota+Bekasi', 'Bekasi+City']
        kab = ['KAB. BEKASI', 'Kab. Bekasi', 'Bekasi Regency', 'Bekasi+Regency']
        for c in kota:
            self.assertEqual(self.code(c, 'West Java Province')[0], '32.75', c)
        for c in kab:
            self.assertEqual(self.code(c, 'Jawa+Barat')[0], '32.16', c)
        self.assertEqual(self.code('Bekasi', '서자와'), ('32.75', 'bare_name_kota', 'bare_name_kota'))
        self.assertEqual(self.code('Kabupaten Kota Bekasi', 'Jawa Barat')[1], 'bare_name_kota')
        self.assertEqual(self.code('ブカシリージェンシー', '西ジャワ')[0], '32.16')

    def test_jakarta(self):
        for c, p in [('Kota Administrasi Jakarta Selatan', 'DKI Jakarta'), ('KOTA JAKARTA SELATAN', 'DKI JAKARTA'),
                     ('Jakarta Selatan', 'Jakarta Raya'), ('South Jakarta', 'Jakarta'),
                     ('South+Jakarta+City', 'Jakarta+Province')]:
            self.assertEqual(self.code(c, p)[0], '31.74', (c, p))
        self.assertEqual(self.code('Central Jakarta', 'Jakarta')[0], '31.71')

    def test_names_with_kota_inside(self):
        self.assertEqual(self.code('KAB. KOTA WARINGIN TIMUR', 'KALIMANTAN TENGAH')[0], '62.02')
        self.assertEqual(self.code('KAB. LIMA PULUH KOTO/KOTA', 'SUMATERA BARAT')[0], '13.07')
        self.assertEqual(self.code('KAB. KOTA BARU', 'KALIMANTAN SELATAN')[0], '63.02')

    def test_renamed_and_spelling(self):
        self.assertEqual(self.code('KAB. MAMUJU UTARA', 'SULAWESI BARAT')[0], '76.01')       # Pasangkayu
        self.assertEqual(self.code('KAB. PONTIANAK', 'KALIMANTAN BARAT')[0], '61.02')        # Mempawah
        self.assertEqual(self.code('KAB. KEPULAUAN SIAU TAGULANDANG BIARO (SITARO)', 'SULAWESI UTARA')[0], '71.09')
        self.assertEqual(self.code('West Tanjung Jabung', 'Jambi')[0], '15.06')

    def test_papua_split(self):
        # Merauke moved to Papua Selatan (93) in 2022; old wording still says Papua
        self.assertEqual(self.code('Kab. Merauke', 'Papua')[0][:2], '93')

    def test_unresolved(self):
        self.assertEqual(self.code('Pulau Bangka', 'Bangka Belitung')[1], 'unresolved')
        self.assertEqual(self.code('', 'Bali')[1], 'unresolved')


class HelpersTest(unittest.TestCase):
    def test_normalise(self):
        self.assertEqual(normalise_city('Kab. Bekasi'), ('KABUPATEN', 'BEKASI'))
        self.assertEqual(normalise_city('Bekasi City'), ('KOTA', 'BEKASI'))
        self.assertEqual(normalise_city('Bekasi'), (None, 'BEKASI'))

    def test_extract_zip(self):
        self.assertEqual(extract_zip('..., KOTA BEKASI, JATIASIH, JAWA BARAT, ID, 17423', 'address'), '17423')
        self.assertEqual(extract_zip('Jl. X No. 3, Kota Jakarta Pusat, DKI Jakarta 10140, Indonesia', 'address'), '10140')
        self.assertEqual(extract_zip('*****', 'field'), None)
        self.assertEqual(extract_zip('10530', 'field'), '10530')
        self.assertEqual(extract_zip('105300', 'field'), None)

    def test_pick_city(self):
        self.assertEqual(pick_city(1, None, 2), 1)          # zip decides
        self.assertEqual(pick_city(1, [2, 3], 2), 2)        # zip in several cities, wording names one
        self.assertEqual(pick_city(1, [3], 2), 1)
        self.assertEqual(pick_city(None, None, 2), 2)       # no known zip -> wording


if __name__ == '__main__':
    unittest.main()
