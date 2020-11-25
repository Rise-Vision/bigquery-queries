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
with

productionCompanies as
(
select 
  C.*
from `rise-core-log.coreData.companies` C
inner join (select max(id) as id, companyId from `rise-core-log.coreData.companies` group by companyId) CC on C.id=CC.id
where C.appId = 's~rvaserver2' and C.isTest = false and not (C.companyId in (select id from `rise-core-log.coreData.deleted` where kind = 'Company'))
),

productionDisplays as
(
select 
  D.*
from `rise-core-log.coreData.displays` D
inner join (select max(id) as id, displayId from `rise-core-log.coreData.displays` group by displayId) DD on D.id = DD.id
where D.appId = 's~rvaserver2' and not (D.displayId in (select id from `rise-core-log.coreData.deleted` where kind = 'Display'))
),

productionSchedules as
(
select 
  S.*
from `rise-core-log.coreData.schedules` S
inner join (select max(id) as id, scheduleId from `rise-core-log.coreData.schedules` group by scheduleId) SS on S.id = SS.id
where S.appId = 's~rvaserver2' and not (S.scheduleId in (select id from `rise-core-log.coreData.deleted` where kind = 'Schedule'))
),

upToDateHierarchy as
(
select 
  H.companyId,
  H.ancestorId
from `rise-core-log.coreData.hierarchy` H
inner join (select max(id) as id, companyId from `rise-core-log.coreData.hierarchy` group by companyId) HH on H.id=HH.id
where H.appId = 's~rvaserver2' 
),

companyNetworks as
(
select
  ancestorId as companyId,
  companyId as subCompanyId
from upToDateHierarchy H, H.ancestorId as ancestorId
union all
select
  companyId as companyId,
  companyId as subCompanyId
from upToDateHierarchy H
),

networkCompanies as
(
select 
  N.companyId, 
  N.subCompanyId,
  C.name, 
  C.companyIndustry
from companyNetworks as N
inner join productionCompanies C on N.companyId = C.companyId
where C.parentId = 'f114ad26-949d-44b4-87e9-8528afc76ce4' -- production Rise Vision Company ID
),

errorCounts as
(
select 
  DATE(timestamp) as date,
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
  eventErrorCode,
  count(*) as errorCount
from `endpoint-event-logs.logs.eventLog`
where DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
),

errorCountsWithCompanyId as 
(
select 
E.*,
case E.endpointType 
  when 'Display' then D.companyId
  else S.companyId
end as companyId
from errorCounts E
left outer join productionDisplays D on E.endpointId = D.displayId
left outer join productionSchedules S on E.scheduleId = S.scheduleId
)

select 
E.date,
E.endpointId,
E.endpointType,
E.licenseStatus,
E.browserVersion,
E.osVersion,
E.playerVersion,
E.viewerVersion,
E.scheduleId,
E.presentationId,
E.placeholderId,
E.componentId,
E.scheduleItemUrl,
E.eventApp,
E.eventAppVersion,
C.companyId,
C.name as companyName,
C.companyIndustry,
P.companyId as parentCompanyId,
P.name as parentCompanyName,
N.companyId as networkCompanyId,
N.name as networkCompanyName,
N.companyIndustry as networkCompanyIndustry,
E.eventErrorCode,
E.errorCount
from errorCountsWithCompanyId E
left outer join productionCompanies C on E.companyId = C.companyId
left outer join productionCompanies P on C.parentId = P.companyId
left outer join networkCompanies N on C.companyId = N.subCompanyId 
