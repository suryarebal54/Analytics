{{config(
    materialized='table',
    schema='bronze'
)}}

-- Create the audit log table if it doesn't exist
SELECT
    1 as record_id,
    'INITIAL_SETUP' as source_table,
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_USER() as processed_by,
    0 as processing_time,
    'SUCCESS' as status
WHERE NOT EXISTS (SELECT 1 FROM {{ this }})
