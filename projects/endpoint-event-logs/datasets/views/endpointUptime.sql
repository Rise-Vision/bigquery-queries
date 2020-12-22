select distinct
  date, 
  endpointId, 
  endpointType, 
  companyId,
  companyName,
  companyIndustry,
  parentCompanyId,
  parentCompanyName,
  networkCompanyId,
  networkCompanyName,
  networkCompanyIndustry,
  uptimePercentage
from endpoint-event-logs.logs.dailyUptime 
where endpointType <> 'Unknown' and companyId is not null and eventApp is null and (date between DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))