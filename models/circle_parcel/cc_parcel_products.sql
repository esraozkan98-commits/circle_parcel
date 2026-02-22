-- Bu model, her paketteki ürünlerin detaylarını gösterir.
-- stg_cc_parcel_products ile cc_parcel'i birleştirerek ürün bazında tablo oluşturur.

{{ config(
    materialized='table'
) }}

SELECT
    -- Anahtarlar --
    p.parcel_id
    ,p.model_name

    -- Paket bilgileri --
    ,c.parcel_tracking
    ,c.transporter
    ,c.priority

    -- Tarihler --
    ,c.date_purchase
    ,c.date_shipping
    ,c.date_delivery
    ,c.date_cancelled

    -- Ay --
    ,c.month_purchase

    -- Durum --
    ,c.status

    -- Zaman hesaplamaları --
    ,c.expedition_time
    ,c.transport_time
    ,c.delivery_time

    -- Gecikme --
    ,c.delay

    -- Metrikler --
    ,p.qty

FROM {{ ref('stg_cc_parcel_products') }} p
LEFT JOIN {{ ref('cc_parcel') }} c USING (parcel_id)