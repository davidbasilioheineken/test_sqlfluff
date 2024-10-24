-- Databricks notebook source
with
-- parse workspaces json
workspace as (select distinct(workspace_id) as workspace_id, null as workspace_name from heiaepmx001dwe01.billing.usage ),
-- apply date filter
usage_with_ws_filtered_by_date as 
(
  select
  /*
    case
      when workspace_name is null then concat('id: ', u.workspace_id)
      else concat(workspace_name, ' (id: ', u.workspace_id, ')')
    end as workspace,
    */
    u.*
  from heiaepmx001dwe01.billing.usage as u
  left join workspace
    on u.workspace_id = workspace.workspace_id
  where u.usage_date between now() and now() - interval 1 day
),
-- apply workspace filter
usage_filtered as 
(
  select
    *
  from usage_with_ws_filtered_by_date
  where if(:param_workspace='<ALL WORKSPACES>', true, workspace = :param_workspace) -- all workspaces under account, or single workspace
),
-- calc list priced usage in USD
prices as (
  select coalesce(price_end_time, date_add(current_date, 1)) as coalesced_price_end_time, *
  from heiaepmx001dwe01.billing.list_prices
  where currency_code = 'USD'
),
list_priced_usd as (
  select
    coalesce(u.usage_quantity * p.pricing.default, 0) as usage_usd,
    date_trunc('QUARTER', usage_date) as usage_quarter,
    date_trunc('MONTH', usage_date) as usage_month,
    date_trunc('WEEK', usage_date) as usage_week,
    u.*
  from usage_filtered as u
  left join prices as p
    on u.sku_name=p.sku_name
    and u.usage_unit=p.usage_unit
    and (u.usage_end_time between p.price_start_time and p.coalesced_price_end_time)
),
-- eval time_key param
list_priced_usd_with_time_key as (
  select
    identifier
    (
      case
        when :param_time_key = 'Quarter' then 'usage_quarter'
        when :param_time_key = 'Month' then 'usage_month'
        when :param_time_key = 'Week' then 'usage_week'
        else 'usage_date'
      end
    ) as time_key,
    *
  from list_priced_usd
),
-- eval group_key param
ws_count as (
  select
    count(distinct(workspace)) as workspace_count
  from list_priced_usd_with_time_key
  where workspace is not null
),
top_workspace_usage as (
  select
    workspace as top_workspace,
    sum(usage_usd) as _top_ws_usage_usd
  from list_priced_usd_with_time_key
  where workspace is not null
  group by top_workspace
  order by _top_ws_usage_usd desc
  limit 10
),
list_priced_usd_with_time_and_group_keys as (
  select
    if(workspace_count <= 50 or workspace is null or top_workspace is not null, workspace, '<OTHERS>') as workspace_norm,
    identifier
    (
      case
        when :param_group_key = 'Workspace' then 'workspace_norm'
        when :param_group_key = 'SKU' then 'sku_name'
        else 'billing_origin_product'
      end
    ) as group_key,
    *
  from ws_count, list_priced_usd_with_time_key u
  left join top_workspace_usage
    on u.workspace = top_workspace_usage.top_workspace
)
-- query
select
  time_key, group_key, usage_usd
from list_priced_usd_with_time_and_group_keys

