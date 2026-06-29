{{ config(materialized='view') }}
select * exclude (_dlt_id, _dlt_load_id)
from {{ source('bronze', 'space_track_conjunction_data_messages') }}
