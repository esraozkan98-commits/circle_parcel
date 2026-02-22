-- Bu model, staging katmanındaki parcel verilerini birleştirerek
-- KPI'lar hesaplar ve son cc_parcel tablosunu oluşturur.

WITH nb_products_parcel AS (
    -- Her paket için toplam ürün adedi ve farklı model sayısını hesaplıyoruz
    SELECT
        parcel_id
        ,SUM(qty) AS qty
        ,COUNT(DISTINCT model_name) AS nb_products
    FROM {{ ref('stg_cc_parcel_products') }}
    GROUP BY parcel_id
)

SELECT
    -- Anahtar --
    p.parcel_id

    -- Paket bilgileri --
    ,p.parcel_tracking
    ,p.transporter
    ,p.priority

    -- Tarihler --
    ,PARSE_DATE("%B %e, %Y", p.date_purchase) AS date_purchase
    ,PARSE_DATE("%B %e, %Y", p.date_shipping) AS date_shipping
    ,PARSE_DATE("%B %e, %Y", p.date_delivery) AS date_delivery
    ,PARSE_DATE("%B %e, %Y", p.date_cancelled) AS date_cancelled

    -- Ay --
    ,EXTRACT(MONTH FROM PARSE_DATE("%B %e, %Y", p.date_purchase)) AS month_purchase

    -- Durum --
    ,CASE
        WHEN p.date_cancelled IS NOT NULL THEN 'İptal Edildi'
        WHEN p.date_shipping IS NULL THEN 'Devam Ediyor'
        WHEN p.date_delivery IS NULL THEN 'Taşınıyor'
        WHEN p.date_delivery IS NOT NULL THEN 'Teslim Edildi'
        ELSE NULL
    END AS status

    -- Zaman hesaplamaları --
    ,DATE_DIFF(PARSE_DATE("%B %e, %Y", p.date_shipping), PARSE_DATE("%B %e, %Y", p.date_purchase), DAY) AS expedition_time
    ,DATE_DIFF(PARSE_DATE("%B %e, %Y", p.date_delivery), PARSE_DATE("%B %e, %Y", p.date_shipping), DAY) AS transport_time
    ,DATE_DIFF(PARSE_DATE("%B %e, %Y", p.date_delivery), PARSE_DATE("%B %e, %Y", p.date_purchase), DAY) AS delivery_time

    -- Gecikme --
    ,IF(p.date_delivery IS NULL, NULL,
        IF(DATE_DIFF(PARSE_DATE("%B %e, %Y", p.date_delivery), PARSE_DATE("%B %e, %Y", p.date_purchase), DAY) > 5, 1, 0)
    ) AS delay

    -- Metrikler --
    ,nb.qty
    ,nb.nb_products AS nb_model

FROM {{ ref('stg_cc_parcel') }} p
LEFT JOIN nb_products_parcel nb USING (parcel_id)