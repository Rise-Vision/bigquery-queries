#standardSQL
-- lists displayId and date from uptime daily summary and uptime heartbeat from the last 7 days (accurate as of time of querying)
select 
  date,
  endpointId as displayId
from
  `endpoint-event-logs.logs.dailyUptime`
where date between DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) and endpointType = 'Display'
union distinct
select
  DATE(timestamp) as date,
  endpointId as displayId
from
  `endpoint-event-logs.heartbeats.uptimeHeartbeats`
where DATE(timestamp) = CURRENT_DATE() and endpointType = 'Display'
