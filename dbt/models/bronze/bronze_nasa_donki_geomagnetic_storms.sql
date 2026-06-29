{{ config(materialized='view') }}
select * exclude (_dlt_id, _dlt_load_id)
from {{ source('bronze', 'nasa_donki_geomagnetic_storms') }}
