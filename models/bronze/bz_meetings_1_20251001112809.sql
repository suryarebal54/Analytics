{{config(
    materialized='table',
    schema='bronze',
    pre_hook="{% if this.identifier != 'bz_audit_log' %}
                {% set start_time = modules.datetime.datetime.now() %}
                {% do run_query('INSERT INTO bronze.bz_audit_log (source_table, load_timestamp, processed_by, processing_time, status) VALUES (\'meetings\', CURRENT_TIMESTAMP(), CURRENT_USER(), 0, \'STARTED\')') %}
              {% endif %}",
    post_hook="{% if this.identifier != 'bz_audit_log' %}
                {% set end_time = modules.datetime.datetime.now() %}
                {% set duration = (end_time - start_time).total_seconds() * 1000 %}
                {% do run_query('INSERT INTO bronze.bz_audit_log (source_table, load_timestamp, processed_by, processing_time, status) VALUES (\'meetings\', CURRENT_TIMESTAMP(), CURRENT_USER(), ' ~ duration ~ ', \'SUCCESS\')') %}
              {% endif %}"
)}}

-- Extract and transform meetings data from raw to bronze
SELECT
    -- Direct mappings from source
    meeting_id,
    host_id,
    meeting_topic,
    start_time,
    end_time,
    duration_minutes,
    -- Metadata columns
    CURRENT_TIMESTAMP() as load_timestamp,
    CURRENT_TIMESTAMP() as update_timestamp,
    'ZOOM_PLATFORM' as source_system
FROM {{ source('raw', 'meetings') }}
