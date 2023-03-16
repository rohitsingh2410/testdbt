
{{ config (
    materialized="table"
)}}
    table "dev".public."vuc__dbt_tmp"
  as (
    
with __dbt__cte__vuc_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "dev".public._airbyte_raw_vuc
select
    jsonb_extract_path_text(_airbyte_data, 'Ad') as ad,
    jsonb_extract_path_text(_airbyte_data, 'CPM') as cpm,
    jsonb_extract_path_text(_airbyte_data, 'Cost') as "Cost",
    jsonb_extract_path_text(_airbyte_data, 'Date') as "Date",
    jsonb_extract_path_text(_airbyte_data, 'Line') as line,
    jsonb_extract_path_text(_airbyte_data, 'Reach') as reach,
    jsonb_extract_path_text(_airbyte_data, 'Status') as status,
    jsonb_extract_path_text(_airbyte_data, 'Campaign') as campaign,
    jsonb_extract_path_text(_airbyte_data, 'Frequency') as frequency,
    jsonb_extract_path_text(_airbyte_data, 'Impressions') as impressions,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from "dev".public._airbyte_raw_vuc as table_alias
-- vuc
where 1 = 1
),  __dbt__cte__vuc_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__vuc_ab1
select
    cast(ad as text) as ad,
    cast(cpm as text) as cpm,
    cast("Cost" as text) as "Cost",
    cast("Date" as text) as "Date",
    cast(line as text) as line,
    cast(reach as 
    float
) as reach,
    cast(status as text) as status,
    cast(campaign as text) as campaign,
    cast(frequency as 
    float
) as frequency,
    cast(impressions as 
    float
) as impressions,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from __dbt__cte__vuc_ab1
-- vuc
where 1 = 1
),  __dbt__cte__vuc_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__vuc_ab2
select
    md5(cast(coalesce(cast(ad as text), '') || '-' || coalesce(cast(cpm as text), '') || '-' || coalesce(cast("Cost" as text), '') || '-' || coalesce(cast("Date" as text), '') || '-' || coalesce(cast(line as text), '') || '-' || coalesce(cast(reach as text), '') || '-' || coalesce(cast(status as text), '') || '-' || coalesce(cast(campaign as text), '') || '-' || coalesce(cast(frequency as text), '') || '-' || coalesce(cast(impressions as text), '') as text)) as _airbyte_vuc_hashid,
    tmp.*
from __dbt__cte__vuc_ab2 tmp
-- vuc
where 1 = 1
)-- Final base SQL model
-- depends_on: __dbt__cte__vuc_ab3
select
    ad,
    cpm,
    "Cost",
    "Date",
    line,
    reach,
    status,
    campaign,
    frequency,
    impressions,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at,
    _airbyte_vuc_hashid
from __dbt__cte__vuc_ab3
-- vuc from "dev".public._airbyte_raw_vuc
where 1 = 1
  );
