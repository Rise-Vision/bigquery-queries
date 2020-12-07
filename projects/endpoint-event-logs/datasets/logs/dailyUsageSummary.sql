#standardSQL
with

productionCompanies as
(
select 
  C.*
from `rise-core-log.coreData.companies` C
inner join (select max(id) as id, companyId from `rise-core-log.coreData.companies` group by companyId) CC on C.id=CC.id
where C.appId = 's~rvaserver2' and C.isTest = false
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
  max(ts) as timestamp,
  display_id as displayId,
  concat(player_name, ' ', player_version)  as playerVersion
from `client-side-events.Player_Data.configuration`
where trim(concat(player_name, ' ', player_version)) <> ''
group by 2, 3
),

licensedDisplays as
(
select
  companyId,
  displayId,
  licensedBy
from (
select
  S.companyId,
  displayId,
  S.companyId as licensedBy
from `rise-core-log.coreData.companySubscriptions` S, S.playerProAssignedDisplays as displayId
inner join (select max(id) as id, companyId from `rise-core-log.coreData.companySubscriptions` group by companyId) SS on S.id = SS.id
where
  (S.planCompanyId is null or S.planCompanyId = "") and
  `rise-core-log.coreData.arrayIndexOf`(S.playerProAssignedDisplays, displayId) < `rise-core-log.coreData.getTotalLicenses`(S.planSubscriptionStatus, S.planCurrentPeriodEndDate, S.planTrialExpiryDate, S.playerProSubscriptionStatus, S.playerProCurrentPeriodEndDate, S.planPlayerProLicenseCount, S.playerProLicenseCount)
union distinct
select
  SC.companyId,
  displayId,
  S.companyId as licensedBy
from `rise-core-log.coreData.companySubscriptions` S, S.playerProTotalAssignedDisplays as displayId
inner join (select max(id) as id, companyId from `rise-core-log.coreData.companySubscriptions` group by companyId) SS on S.id = SS.id
inner join `rise-core-log.coreData.companySubscriptions` SC on S.companyId = SC.planCompanyId
inner join (select max(id) as id, companyId from `rise-core-log.coreData.companySubscriptions` group by companyId) SSS on SC.id = SSS.id
inner join productionDisplays D on SC.companyId = D.companyId and displayId = D.displayId
where
  `rise-core-log.coreData.arrayIndexOf`(S.playerProTotalAssignedDisplays, displayId) < `rise-core-log.coreData.getTotalLicenses`(S.planSubscriptionStatus, S.planCurrentPeriodEndDate, S.planTrialExpiryDate, S.playerProSubscriptionStatus, S.playerProCurrentPeriodEndDate, S.planPlayerProLicenseCount, S.playerProLicenseCount)
)
),

heartbeatCounts as
(
select 
  DATE(timestamp) as date,
  endpointId,
  scheduleId,
  presentationId,
  placeholderId,
  componentId,
  scheduleItemUrl,
  eventApp,
  eventAppVersion,
  count(*) as heartbeatCount
from `endpoint-event-logs.heartbeats.uptimeHeartbeats`
where DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
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
  scheduleItemUrl,
  eventApp,
  eventAppVersion
from `endpoint-event-logs.heartbeats.uptimeHeartbeats`
where DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) and
eventApp = 'Viewer'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
),

usage as 
(
select 
M.*,
case M.endpointType 
  when 'Display' then D.companyId
  else S.companyId
end as companyId,
P.productCode as templateId,
H.heartbeatCount / 288 as usage
from mentions M
left outer join productionDisplays D on M.endpointId = D.displayId
left outer join productionSchedules S on M.scheduleId = S.scheduleId
left outer join productionPresentations P on M.presentationId = P.presentationId
left outer join heartbeatCounts H on M.date = H.date and M.endpointId = H.endpointId and M.scheduleId = H.scheduleId and ifnull(M.presentationId, '') = ifnull(H.presentationId, '') and
  ifnull(M.placeholderId, '') = ifnull(H.placeholderId, '') and ifnull(M.componentId, '') = ifnull(H.componentId, '') and ifnull(M.scheduleItemUrl, '') = ifnull(H.scheduleItemUrl, '') and 
  M.eventApp = H.eventApp and M.eventAppVersion = H.eventAppVersion
)

select 
U.date,
U.endpointId,
U.endpointType,
if(L.displayId is null, 'Unlicensed', 'Licensed') as licenseStatus,
U.browserVersion,
U.osVersion,
V.playerVersion,
U.viewerVersion,
U.scheduleId,
U.presentationId,
U.templateId,
U.componentId,
U.scheduleItemUrl,
U.eventApp,
U.eventAppVersion,
C.companyId,
C.name as companyName,
C.companyIndustry,
P.companyId as parentCompanyId,
P.name as parentCompanyName,
N.companyId as networkCompanyId,
N.name as networkCompanyName,
N.companyIndustry as networkCompanyIndustry,
U.usage
from usage U
left outer join productionCompanies C on U.companyId = C.companyId
left outer join productionCompanies P on C.parentId = P.companyId
left outer join networkCompanies N on C.companyId = N.subCompanyId 
left outer join playerVersions V on U.endpointId = V.displayId
left outer join licensedDisplays L on U.endpointId = L.displayId
