{#
    dbt'nin varsayilan generate_schema_name macro'su, custom schema'lari
    "{{target.schema}}_{{custom_schema}}" formatinda birlestirir; sonuc:
    "ecommerce_lakehouse.silver" gibi gecersiz tablo adlari uretir.

    Bu override custom schema verildiyse aynen kullanir, yoksa target.schema'ya
    duser. Boylece "lakehouse.silver" three-part name dogrulu kalir.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
