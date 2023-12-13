{% set partitions_to_replace = ["current_date"] %}
{% for i in range(var("static_incremental_days")) %}
    {# Directly append to the list without reassignment #}
    {% do partitions_to_replace.append(
        "date_sub(current_date, interval " + (i + 1) | string + " day)"
    ) %}
{% endfor %}

{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "session_start_date",
            "data_type": "date",
        },
        partitions=partitions_to_replace
    )
}}

SELECT
base.session_partition_date as session_start_date,
session_number,
session_default_channel_grouping,
session_medium,
session_source,
session_campaign,
session_content,
landing_page_path,
device_category,
device_operating_system,
device_web_info_browser,
device_language,
most_recent_traffic_type,
geo_country,
geo_region,
count(distinct session_key) as sessions,
sum(session_partition_count_page_views) as pageviews,
count(distinct case when view_item_count > 0 then session_key end) as view_item_sessions,
count(distinct case when add_to_cart_count > 0 then session_key end) as add_to_cart_sessions,
count(distinct case when begin_checkout_count > 0 then session_key end) as begin_checkout_sessions,
count(distinct case when purchase_count > 0 then session_key end) as purchases
FROM
  {{ ref('fct_ga4__sessions_daily') }} base
left join {{ ref('dim_ga4__sessions_daily') }}
using(session_key)
{% if is_incremental() %}
where base.session_partition_date > (select max(session_start_date) from {{ this }})

{% endif %}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15