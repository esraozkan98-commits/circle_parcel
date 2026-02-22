-- Bu model, staging katmanındaki parcel verilerini birleştirerek
-- KPI'lar hesaplar ve son cc_parcel tablosunu oluşturur.
WITH nb_products_parcel AS (
    SELECT
        parcel_id
        ,SUM(qty) AS qty
        ,COUNT(DISTINCT model_name) AS nb_products
    FROM {{ ref('stg_cc_parcel_products') }}
    GROUP BY parcel_id
)
SELECT
    p.parcel_id
    ,p.parcel_tracking
    ,p.transporter
    ,p.priority
    ,CAST(p.date_purchase AS DATE) AS date_purchase
    ,CAST(p.date_shipping AS DATE) AS date_shipping
    ,CAST(p.date_delivery AS DATE) AS date_delivery
    ,CAST(p.date_cancelled AS DATE) AS date_cancelled
    ,EXTRACT(MONTH FROM CAST(p.date_purchase AS DATE)) AS month_purchase
    ,CASE
        WHEN p.date_cancelled IS NOT NULL THEN 'İptal Edildi'
        WHEN p.date_shipping IS NULL THEN 'Devam Ediyor'
        WHEN p.date_delivery IS NULL THEN 'Taşınıyor'
        WHEN p.date_delivery IS NOT NULL THEN 'Teslim Edildi'
        ELSE NULL
    END AS status
    ,DATE_DIFF(CAST(p.date_shipping AS DATE), CAST(p.date_purchase AS DATE), DAY) AS expedition_time
    ,DATE_DIFF(CAST(p.date_delivery AS DATE), CAST(p.date_shipping AS DATE), DAY) AS transport_time
    ,DATE_DIFF(CAST(p.date_delivery AS DATE), CAST(p.date_purchase AS DATE), DAY) AS delivery_time
    ,IF(p.date_delivery IS NULL, NULL,
        IF(DATE_DIFF(CAST(p.date_delivery AS DATE), CAST(p.date_purchase AS DATE), DAY) > 5, 1, 0)
    ) AS delay
    ,nb.qty
    ,nb.nb_products AS nb_model
FROM {{ ref('stg_cc_parcel') }} p
LEFT JOIN nb_products_parcel nb USING (parcel_id)