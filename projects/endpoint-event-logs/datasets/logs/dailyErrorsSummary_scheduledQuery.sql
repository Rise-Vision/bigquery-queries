#standardSQL
insert into `endpoint-event-logs.logs.dailyErrors` 
(
date,
endpointId,
endpointType,
licenseStatus,
browserVersion, 
osVersion, 
playerVersion, 
viewerVersion, 
scheduleId, 
presentationId,
placeholderId,
componentId,
scheduleItemUrl,
eventApp,
eventAppVersion,
companyId,
companyName,
companyIndustry,
parentCompanyId,
parentCompanyName,
networkCompanyId,
networkCompanyName,
networkCompanyIndustry,
eventErrorCode,
errorCount
)
select 
  date,
  endpointId,
  endpointType,
  licenseStatus,
  browserVersion, 
  osVersion, 
  playerVersion, 
  viewerVersion, 
  scheduleId, 
  presentationId,
  placeholderId,
  componentId,
  scheduleItemUrl,
  eventApp,
  eventAppVersion,
  companyId,
  companyName,
  companyIndustry,
  parentCompanyId,
  parentCompanyName,
  networkCompanyId,
  networkCompanyName,
  networkCompanyIndustry,
  eventErrorCode,
  errorCount
from `endpoint-event-logs.logs.dailyErrorsSummary`