#standardSQL
with

productionCompanies as
(
select 
  C.*
from `rise-core-log.coreData.companies` C
inner join (select max(id) as id, companyId from `rise-core-log.coreData.companies` group by companyId) CC on C.id=CC.id
where C.appId = 's~rvaserver2'
),

productionDisplays as
(
select 
  D.*
from `rise-core-log.coreData.displays` D
inner join (select max(id) as id, displayId from `rise-core-log.coreData.displays` group by displayId) DD on D.id = DD.id
where D.appId = 's~rvaserver2'
),

productionSchedules as
(
select 
  S.*
from `rise-core-log.coreData.schedules` S
inner join (select max(id) as id, scheduleId from `rise-core-log.coreData.schedules` group by scheduleId) SS on S.id = SS.id
where S.appId = 's~rvaserver2'
),

productionPresentations as
(
select 
  P.*
from `rise-core-log.coreData.presentations` P
inner join (select max(id) as id, presentationId from `rise-core-log.coreData.presentations` group by presentationId) PP on P.id = PP.id
where P.appId = 's~rvaserver2'
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

playerVersions as
(
select 
  C.display_id as displayId,
  concat(player_name, ' ', player_version)  as playerVersion
from `client-side-events.Player_Data.configuration` C
inner join (select max(ts) as ts, display_id from `client-side-events.Player_Data.configuration` group by 2) CC on C.ts = CC.ts and C.display_id = CC.display_id
group by 1, 2
),

mentions as
(
select 
  DATE(timestamp) as date,
  endpointId,
  endpointType,
  browserVersion,
  osVersion,
  eventAppVersion as viewerVersion,
  scheduleId,
  presentationId,
  placeholderId,
  componentId,
  scheduleItemUrl
from `endpoint-event-logs.heartbeats.uptimeHeartbeats`
where DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) and
eventApp = 'Viewer'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
),

uptimeErrors as
(
select 
  TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(timestamp), 300) * 300) as intervalStart,
  L.endpointId,
  `endpoint-event-logs.logs.cleanUpEventApp`(L.eventApp) as eventApp,
  L.eventAppVersion,
  count(L.eventErrorCode) as errorCount
from `endpoint-event-logs.logs.eventLog` L
inner join `endpoint-event-logs.errors.errorDefinitions` E on L.eventErrorCode = E.code 
where DATE(L.timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
group by 1, 2, 3, 4
),

uptimeIntervals as
(
select 
  TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(timestamp), 300) * 300) as intervalStart,
  endpointId,
  `endpoint-event-logs.logs.cleanUpEventApp`(eventApp) as eventApp,
  eventAppVersion
from `endpoint-event-logs.heartbeats.uptimeHeartbeats`
where DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
union distinct
select
  TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(timestamp), 300) * 300) as intervalStart,
  endpointId,
  `endpoint-event-logs.logs.cleanUpEventApp`(eventApp) as eventApp,
  eventAppVersion
from `endpoint-event-logs.logs.eventLog`
where DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),

uptimeAndDowntime as
(
select 
  I.*,
  IF(E.errorCount is null or E.errorCount = 0, "up", "down") as intervalStatus
from uptimeIntervals I
left outer join uptimeErrors E on I.intervalStart = E.intervalStart and I.endpointId = E.endpointId and I.eventApp = E.eventApp and I.eventAppVersion = E.eventAppVersion
),

endpointAppIntervalCount as
(
select
  endpointId,
  eventApp,
  eventAppVersion,
  count(*) as endpointAppIntervals
from uptimeAndDowntime
group by 1, 2, 3
),

endpointIntervalCount as
(
select
  endpointId,
  count(*) as endpointIntervals
from uptimeAndDowntime
group by 1
),

endpointAppUpIntervalCount as
(
select
  endpointId,
  eventApp,
  eventAppVersion,
  count(*) as endpointAppUpIntervals
from uptimeAndDowntime
where intervalStatus = 'up'
group by 1, 2, 3
),

endpointUpIntervalCount as
(
select
  endpointId,
  count(*) as endpointUpIntervals
from uptimeAndDowntime
where intervalStatus = 'up'
group by 1
),

endpointUptime as (
select 
  T.endpointId,
  null as eventApp,
  ifnull(endpointUpIntervals, 0) / ifnull(endpointIntervals, 1) as uptimePercentage
from endpointIntervalCount T
left outer join endpointUpIntervalCount U on T.endpointId = U.endpointId
), 

endpointAppUptime as (
select 
  T.endpointId,
  T.eventApp,
  T.eventAppVersion, 
  ifnull(endpointAppUpIntervals, 0) / ifnull(endpointAppIntervals, 1) as uptimePercentage
from endpointAppIntervalCount T
left outer join endpointAppUpIntervalCount U on T.endpointId = U.endpointId and T.eventApp = U.eventApp and T.eventAppVersion = U.eventAppVersion
),

endpointUptimeWithFields as (
select
  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) as date,
  M.endpointId,
  M.endpointType,
  M.browserVersion,
  M.osVersion,
  M.viewerVersion,
  M.scheduleId,
  M.presentationId,
  M.placeholderId,
  P.productCode as templateId,
  M.componentId,
  M.scheduleItemUrl,
  '' as eventApp,
  '' as eventAppVersion,
  case M.endpointType 
    when 'Display' then D.companyId
    else S.companyId
  end as companyId,
  U.uptimePercentage
from mentions M
inner join endpointUptime U on M.endpointId = U.endpointId
left outer join productionDisplays D on M.endpointId = D.displayId
left outer join productionSchedules S on M.scheduleId = S.scheduleId
left outer join productionPresentations P on M.presentationId = P.presentationId
),

endpointAppUptimeWithFields as (
select
  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) as date,
  M.endpointId,
  M.endpointType,
  M.browserVersion,
  M.osVersion,
  M.viewerVersion,
  M.scheduleId,
  M.presentationId,
  M.placeholderId,
  P.productCode as templateId,
  M.componentId,
  M.scheduleItemUrl,
  U.eventApp,
  U.eventAppVersion,
  case M.endpointType 
    when 'Display' then D.companyId
    else S.companyId
  end as companyId,
  U.uptimePercentage
from mentions M
inner join endpointAppUptime U on M.endpointId = U.endpointId
left outer join productionDisplays D on M.endpointId = D.displayId
left outer join productionSchedules S on M.scheduleId = S.scheduleId
left outer join productionPresentations P on M.presentationId = P.presentationId
),

uptimeWithFields as (
 select * from endpointUptimeWithFields
 union all
 select * from endpointAppUptimeWithFields
)

select 
  date,
  endpointId, 
  endpointType, 
  if(L.displayId is null, 'Unlicensed', if(L.planSubscriptionStatus <> 'Suspended', 'Licensed', 'Suspended')) as licenseStatus,
  browserVersion, 
  osVersion, 
  V.playerVersion, 
  viewerVersion, 
  scheduleId, 
  presentationId, 
  templateId, 
  componentId, 
  scheduleItemUrl, 
  nullif(eventApp, '') as eventApp, 
  nullif(eventAppVersion, '') as eventAppVersion, 
  U.companyId, 
  C.name as companyName,
  C.companyIndustry,
  P.companyId as parentCompanyId,
  P.name as parentCompanyName,
  N.companyId as networkCompanyId,
  N.name as networkCompanyName,
  N.companyIndustry as networkCompanyIndustry,
  uptimePercentage
from uptimeWithFields U
left outer join productionCompanies C on U.companyId = C.companyId
left outer join productionCompanies P on C.parentId = P.companyId
left outer join networkCompanies N on C.companyId = N.subCompanyId
left outer join playerVersions V on U.endpointId = V.displayId
left outer join rise-core-log.coreData.licensedDisplays L on U.endpointId = L.displayId