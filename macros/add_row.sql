{% macro add_row() %}

{% set sql %}
create table if not exists analytics.dbt_smcintyre.pet_sales (
    sold_at timestamp_tz,
    pet_name varchar
);

insert into analytics.dbt_smcintyre.pet_sales values (
    {{ current_timestamp() }}, 'Miffy'
);
{% endset %}

{% do run_query(sql) %}

{% endmacro %}
