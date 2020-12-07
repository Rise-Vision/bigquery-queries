#standardSQL
insert into `endpoint-event-logs.logs.dailyUsage` 
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
templateId,
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
usage
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
  templateId,
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
  usage
from `endpoint-event-logs.logs.dailyUsageSummary`