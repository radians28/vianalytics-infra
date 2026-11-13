"""Per-channel stg column maps for the order load.

Copied from vianalytics-svc/sku_consolidation/common/config.py (STG_MAP, PY_TS) so the DAG
builds t_order / t_order_detail exactly like the offline step 6. Keep both in sync.
Only here: `zip` / `zip_style` (postal code for m_zipcode, see orders.city.extract_zip).
"""

# upload channel -> (m_channel.channel_code, stg table) loaded by handle_order_dag_{channel}
SOURCES = {
    'tiktok_tokped': [('TIKTOK', 'stg_tiktok'), ('TOKOPEDIA', 'stg_tokopedia')],
    'shopee': [('SHOPEE', 'stg_shopee')],
    'lazada': [('LAZADA', 'stg_lazada')],
    'blibli': [('BLIBLI', 'stg_blibli')],
}

# timestamp text format of each export (Asia/Jakarta wall clock)
PY_TS = {
    'SHOPEE': '%Y-%m-%d %H:%M',
    'TOKOPEDIA': '%d/%m/%Y %H:%M:%S',
    'TIKTOK': '%d/%m/%Y %H:%M:%S',
    'LAZADA': '%d %b %Y %H:%M',
    'BLIBLI': '%d/%m/%Y %H:%M',
}

# stg columns carrying the facts used downstream.
#   money: 'dot' = '414.090' / 'Rp391.000' style (dot = thousands), 'plain' = '175898.00'
_TIKTOK_TOKOPEDIA = dict(
    money='plain',
    order_no='order_id', line_key=None, created='created_time',
    paid='paid_time', shipped='shipped_time', completed='delivered_time',
    status='order_status', item_status='order_substatus',
    seller_sku='seller_sku', platform_sku='sku_id', product_name='product_name',
    variation='variation', qty='quantity', return_qty='sku_quantity_of_return',
    unit_price='sku_unit_original_price', seller_discount='sku_seller_discount',
    platform_discount='sku_platform_discount', buyer='buyer_username',
    warehouse='warehouse_name', delivery='delivery_option', payment='payment_method',
    city='regency_and_city', province='province', zip='zipcode', zip_style='field',   # masked '*****' today
    shipping_fee='original_shipping_fee', order_total='order_amount')

STG_MAP = {
    'SHOPEE': dict(
        money='dot',
        order_no='no_pesanan', line_key=None, created='waktu_pesanan_dibuat',
        paid='waktu_pembayaran_dilakukan', shipped='waktu_pengiriman_diatur',
        completed='waktu_pesanan_selesai', status='status_pesanan', item_status='status_pesanan',
        seller_sku='nomor_referensi_sku', platform_sku=None, product_name='nama_produk',
        variation='nama_variasi', qty='jumlah', return_qty='returned_quantity',
        unit_price='harga_awal', seller_discount='diskon_dari_penjual',
        platform_discount='diskon_dari_shopee', buyer='username_pembeli',
        warehouse='nama_gudang', delivery='opsi_pengiriman', payment='metode_pembayaran',
        city='kota_kabupaten', province='provinsi', zip='alamat_pengiriman', zip_style='address',
        shipping_fee='ongkos_kirim_dibayar_oleh_pembeli', order_total='total_pembayaran'),
    'LAZADA': dict(
        money='plain',
        order_no='order_number', line_key='order_item_id', created='create_time',
        paid=None, shipped=None, completed='delivered_date', status='status', item_status='status',
        seller_sku='seller_sku', platform_sku='lazada_sku', product_name='item_name',
        variation='variation', qty=None, return_qty=None,
        unit_price='unit_price', seller_discount='seller_discount_total',
        platform_discount='platform_discount_total', buyer='customer_name',
        warehouse='ware_house', delivery='shipping_provider', payment='pay_method',
        city='shipping_city', province='shipping_address3', zip='shipping_post_code', zip_style='field',
        shipping_fee='shipping_fee', order_total='paid_price'),
    'BLIBLI': dict(
        money='dot',
        order_no='nomor_pesanan', line_key='nomor_item_pesanan', created='tanggal_pesanan',
        paid=None, shipped='tanggal_pengiriman', completed=None, status='status_pesanan',
        item_status='status_pesanan', seller_sku='sku_seller', platform_sku='sku_blibli',
        product_name='nama_produk', variation=None, qty='total_produk', return_qty=None,
        unit_price='harga_produk', seller_discount='diskon_produk_ditanggung_seller',
        platform_discount='diskon_produk_ditanggung_blibli', buyer='nama_pelanggan',
        warehouse='lokasi_pengambilan', delivery='nama_logistik', payment=None,
        city='kota', province='provinsi', zip='alamat_pengiriman', zip_style='address',
        shipping_fee=None, order_total='total'),
    'TIKTOK': _TIKTOK_TOKOPEDIA,
    'TOKOPEDIA': _TIKTOK_TOKOPEDIA,
}

# STG_MAP keys that hold stg column names (the rest are settings)
COLUMN_KEYS = ('order_no', 'line_key', 'created', 'paid', 'shipped', 'completed', 'status', 'item_status',
               'seller_sku', 'platform_sku', 'product_name', 'variation', 'qty', 'return_qty', 'unit_price',
               'seller_discount', 'platform_discount', 'buyer', 'warehouse', 'delivery', 'payment', 'city',
               'province', 'zip', 'shipping_fee', 'order_total')


def stg_column_names(channel_code: str) -> list[str]:
    m = STG_MAP[channel_code]
    return sorted({m[k] for k in COLUMN_KEYS if m[k]})
