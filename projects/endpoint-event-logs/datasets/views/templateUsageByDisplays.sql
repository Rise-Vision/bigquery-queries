#standardSQL
-- lists displayId, templateId and date from usage daily summary from the last 7 days (accurate as of yesterday end of day)
select distinct 
  date,
  endpointId as displayId,
  templateId
from
  `endpoint-event-logs.logs.dailyUsage`
where date between DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) and endpointType = 'Display' and trim(ifnull(templateId, '')) <> ''
