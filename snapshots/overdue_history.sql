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
    model_name || '|' || column_name || '|' || check_strategy as id,
    *
from {{ ref('overdue') }}

{% endsnapshot %}
