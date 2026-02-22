-- Bu model, BigQuery'deki ham parcel verisini staging katmanına taşır.
-- Sütun adlarını düzgün formata çeviriyoruz.

{{ config(schema='parcel_dbt_dev_staging') }}

WITH source AS (
    SELECT * FROM {{ source('raw_data_circle', 'raw_cc_parcel') }}
)

SELECT
    Parcel_id AS parcel_id
    ,Parcel_tracking AS parcel_tracking
    ,Transporter AS transporter
    ,priority
    ,PARSE_DATE("%B %e, %Y", Date_purCHase) AS date_purchase
    ,PARSE_DATE("%B %e, %Y", Date_sHIpping) AS date_shipping
    ,PARSE_DATE("%B %e, %Y", DATE_delivery) AS date_delivery
    ,PARSE_DATE("%B %e, %Y", DaTeCANcelled) AS date_cancelled
FROM source