{%- set sqls = [] -%}

{%- for node_name, node in graph.nodes.items() -%}
    {%- for column_name, column_node in node.columns.items() -%}
        {%- if column_node.meta.daily_overdue_check -%}
            {{ sqls.append(daily_overdue_check(node.database ~ '.' ~ node.schema ~ '.' ~ node.alias, column_name, column_node.meta.daily_overdue_check)) or "" }}
        {%- endif -%}
        {%- if column_node.meta.overdue_freshness_check -%}
            {{ sqls.append(overdue_freshness_check(node.database ~ '.' ~ node.schema ~ '.' ~ node.alias, column_name, column_node.meta.overdue_freshness_check)) or "" }}
        {%- endif -%}
    {%- endfor -%}
{%- endfor -%}

{{ sqls | join('UNION ALL') }}
