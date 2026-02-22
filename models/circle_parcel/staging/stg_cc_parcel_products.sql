-- Bu model, BigQuery'deki ham parcel ürün verisini staging katmanına taşır.
-- Sadece ihtiyacımız olan sütunları seçiyoruz.

WITH source AS (
    -- Ham veriyi raw_data_circle dataset'indeki raw_cc_parcel_product tablosundan çekiyoruz
    SELECT * FROM {{ source('raw_data_circle', 'raw_cc_parcel_product') }}
)

SELECT
    ParCEL_id AS parcel_id      -- paketin benzersiz kimliği (foreign key)
    ,Model_mAME AS model_name   -- ürün model adı
    ,QUANTITY AS qty            -- adet
FROM source