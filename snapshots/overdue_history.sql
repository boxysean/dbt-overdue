{% snapshot overdue_history %}

{{
    config(
      target_database='analytics',
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='checked_at',
    )
}}

select 
    fully_qualified_table_name || '|' || column_name || '|' || overdue_strategy as id,
    *
from {{ ref('overdue') }}

{% endsnapshot %}
