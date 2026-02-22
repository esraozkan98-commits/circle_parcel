# Circle Parcel - dbt Project

This project models parcel delivery data for Circle using dbt and BigQuery.

## Project Structure
```
models/
  circle_parcel/
    staging/
      stg_cc_parcel.sql
      stg_cc_parcel_products.sql
      stg_schema.yml
    cc_parcel.sql
    cc_parcel_products.sql
    schema.yml
```

## Data Sources

Raw data is sourced from the `raw_data_circle` dataset in BigQuery:
- `raw_cc_parcel` - Raw parcel delivery data
- `raw_cc_parcel_product` - Raw parcel product data

## Models

### Staging Layer (`parcel_dbt_dev_staging`)
- `stg_cc_parcel` - Cleaned and renamed columns from raw parcel data
- `stg_cc_parcel_products` - Cleaned and renamed columns from raw parcel product data

### Core Layer (`parcel_dbt_dev`)
- `cc_parcel` - Parcel KPIs including delivery status, time metrics and delay flags
- `cc_parcel_products` - Product-level parcel details, partitioned by purchase date

## How to Run
```bash
# Run all models
dbt run

# Run models and tests
dbt build
```

## Tests

Primary key tests are applied to all models to ensure data quality.
