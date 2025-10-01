{% macro generate_audit_insert(source_table) %}
    INSERT INTO {{ ref('bz_audit_log') }} (
        source_table,
        load_timestamp,
        processed_by,
        processing_time,
        status
    )
    SELECT
        '{{ source_table }}' as source_table,
        CURRENT_TIMESTAMP() as load_timestamp,
        CURRENT_USER() as processed_by,
        DATEDIFF('MILLISECOND', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()) as processing_time,
        'SUCCESS' as status
{% endmacro %}
