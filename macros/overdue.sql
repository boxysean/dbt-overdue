-- TODO: timezones.

{# {% macro overdue_freshness_check(model_name, column_name, data_freshness_hours) %} #} {# refs are weird #}
{% macro overdue_freshness_check(fully_qualified_table_name, column_name, data_freshness_hours) %}

select
    '{{ fully_qualified_table_name }}' as fully_qualified_table_name,
    '{{ column_name }}' as column_name,
    'freshness' as check_strategy,
    (select {{ dbt_date.now('UTC') }}) as checked_at,
    {# (select max(to_timestamp({{ column_name }})) from {{ ref(model_name) }}) as high_watermark, #}
    (select max(to_timestamp({{ column_name }})) from {{ fully_qualified_table_name }}) as high_watermark,
    {{ dbt.dateadd('hours', data_freshness_hours, 'checked_at') }} as next_overdue_at,
    {{ datediff('high_watermark', 'next_overdue_at', 'minutes') }} as overdue_minutes,
    overdue_minutes > 0 as is_overdue

{% endmacro %}


{# {% macro daily_overdue_check(model_name, column_name, daily_overdue_hour) %} #}  {# refs are weird #}
{% macro daily_overdue_check(fully_qualified_table_name, column_name, daily_overdue_hour) %}

select
    '{{ fully_qualified_table_name }}' as fully_qualified_table_name,
    '{{ column_name }}' as column_name,
    'daily_overdue_hour' as overdue_strategy,
    (select {{ dbt_date.now('UTC') }}) as checked_at,
    {# (select max(to_timestamp({{ column_name }})) from {{ ref(model_name) }}) as high_watermark, #}
    (select max(to_timestamp({{ column_name }})) from {{ fully_qualified_table_name }}) as high_watermark,
    {{ dbt.dateadd('hours', daily_overdue_hour, date_trunc('day', 'high_watermark')) }} as next_overdue_at,
    {{ datediff('next_overdue_at', 'checked_at', 'minutes') }} as overdue_minutes,
    overdue_minutes > 0 as is_overdue

{% endmacro %}