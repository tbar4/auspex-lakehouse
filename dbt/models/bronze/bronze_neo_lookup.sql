{{ config(materialized='view') }}
select * exclude (_dlt_id, _dlt_load_id)
from {{ source('bronze', 'neo_lookup') }}
