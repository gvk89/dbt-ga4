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
            "field": "event_date_dt",
            "data_type": "date",
        },
        partitions=partitions_to_replace
    )
}}

with items as (
SELECT
items.event_key,
items.event_date_dt,
items.item_id,
items.item_name,
items.item_category,
items.price,
items.quantity,
items.item_revenue,
event_name
FROM 
  {{ ref('stg_ga4__event_items') }} items
where event_name in ('purchase','add_to_cart','view_item','begin_checkout')
    {% if is_incremental() %}
        and event_date_dt > (select max(event_date_dt) from {{ this }})
    {% endif %}
),
session_keys as (
  SELECT event_key,session_key
  from {{ ref('stg_ga4__events') }}
    {% if is_incremental() %}
        where event_date_dt > (select max(event_date_dt) from {{ this }})
    {% endif %}
)
,
session_dimensions as (
  select 
  session_key,
  most_recent_traffic_type,
  last_non_direct_default_channel_grouping,
  last_non_direct_source,
  last_non_direct_medium,
  last_non_direct_campaign,
  last_non_direct_content,
  landing_page_path,
  geo_country,
  geo_region,
  device_category,
  device_operating_system,
  device_web_info_browser,
  device_language
  from {{ ref('dim_ga4__sessions_daily') }}
    {% if is_incremental() %}
        where event_date_dt > (select max(session_start_date) from {{ this }})
    {% endif %}
),
merged as (
  select 
  items.*,
  session_keys,session_key,
  most_recent_traffic_type,
  last_non_direct_default_channel_grouping,
  last_non_direct_source,
  last_non_direct_medium,
  last_non_direct_campaign,
  last_non_direct_content,
  landing_page_path,
  geo_country,
  geo_region,
  device_category,
  device_operating_system,
  device_web_info_browser,
  device_language
  from items
  left join session_keys
  using(event_key)
  left join session_dimensions
  using(session_key)
),
final as (
select
event_date_dt,
most_recent_traffic_type,
last_non_direct_default_channel_grouping,
last_non_direct_source,
last_non_direct_medium,
last_non_direct_campaign,
last_non_direct_content,
landing_page_path,
geo_country,
geo_region,
device_category,
device_operating_system,
device_web_info_browser,
device_language,
item_id,
item_name,
item_category,
price,
quantity,
sum(item_revenue) as item_revenue,
COUNTIF(event_name = 'purchase') AS purchase_count,
COUNTIF(event_name = 'add_to_cart') AS add_to_cart_count,
COUNTIF(event_name = 'view_item') AS item_view_count,
COUNTIF(event_name = 'begin_checkout') AS begin_checkout_count
from merged
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
)

select * from final