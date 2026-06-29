{{ config(materialized='view') }}
select * exclude (_dlt_id, _dlt_load_id)
from {{ source('bronze', 'nasa_astronomy_picture_of_the_day') }}
