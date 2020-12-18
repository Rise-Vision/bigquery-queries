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

downloadCost as
(
select sum(cost) as totalDownloadCost from `rise-core-log.billingData.gcp_billing_export_v1_00FE29_4FDD82_EF884E` 
WHERE (DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) or DATE(_PARTITIONTIME) = CURRENT_DATE()) and date(usage_end_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND project.id = 'avid-life-623' and service.description = 'Cloud Storage' and starts_with(sku.description, 'Download')
),

downloadBandwidth as
(
select 
  sum(sc_bytes) as totalDownloaded,
from `avid-life-623.RiseStorageLogs_v2.UsageLogs*`
where _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d",DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
),

endpointDownloads as
(
select 
REGEXP_EXTRACT(cs_uri, r'endpointId=([^?&#]*)') as endpointId,
REGEXP_EXTRACT(cs_uri, r'endpointType=([^?&#]*)') as endpointType,
REGEXP_EXTRACT(cs_uri, r'scheduleId=([^?&#]*)') as scheduleId,
sum(sc_bytes) as downloadedBytes
from `avid-life-623.RiseStorageLogs_v2.UsageLogs*`
where _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d",DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) and strpos(cs_uri, 'endpointId=') > 0 and strpos(cs_uri, 'endpointType=') > 0 and strpos(cs_uri, 'scheduleId=') > 0
group by 1, 2, 3
),


streamInsertCost as
(
select sum(cost) as totalStreamInsertCost from `rise-core-log.billingData.gcp_billing_export_v1_00FE29_4FDD82_EF884E` 
WHERE (DATE(_PARTITIONTIME) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) or DATE(_PARTITIONTIME) = CURRENT_DATE()) and date(usage_end_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) and project.id = 'endpoint-event-logs' and service.description = 'BigQuery' and sku.description = 'Streaming Insert'
),

streamInsertCount as
(
select count(*) as totalInsertCount
from `endpoint-event-logs.logs.eventLog`
where DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),

endpointInserts as
(
select 
  endpointId,
  endpointType,
  scheduleId,
  count(*) as insertCount
from `endpoint-event-logs.logs.eventLog`
where DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
group by 1, 2, 3
),

endpointMentions as
(
select 
  DATE(timestamp) as date,
  endpointId,
  endpointType,
  browserVersion,
  osVersion,
  eventAppVersion as viewerVersion,
  scheduleId
from `endpoint-event-logs.heartbeats.uptimeHeartbeats`
where DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
group by 1, 2, 3, 4, 5, 6, 7
),

costs as
(
select
  M.*,
  case M.endpointType 
    when 'Display' then D.companyId
    else S.companyId
  end as companyId,
  IFNULL(B.downloadedBytes * totalDownloadCost / totalDownloaded, 0.0) as dailyDirectCost,
  IFNULL(I.insertCount * totalStreamInsertCost / totalInsertCount, 0.0) as dailyIndirectCost
from endpointMentions M, downloadBandwidth, downloadCost, streamInsertCost, streamInsertCount
left outer join endpointDownloads B on M.endpointId = B.endpointId and M.endpointType = B.endpointType and M.scheduleId = B.scheduleId
left outer join endpointInserts I on M.endpointId = I.endpointId and M.endpointType = I.endpointType and M.scheduleId = I.scheduleId
left outer join productionDisplays D on M.endpointId = D.displayId
left outer join productionSchedules S on M.scheduleId = S.scheduleId
)

select 
  CO.date,
  CO.endpointId,
  CO.endpointType,
  if(L.displayId is null, 'Unlicensed', if(L.planSubscriptionStatus <> 'Suspended', 'Licensed', 'Suspended')) as licenseStatus,
  CO.browserVersion,
  V.playerVersion,
  CO.viewerVersion,
  CO.osVersion,
  CO.scheduleId,
  C.companyId,
  C.name as companyName,
  C.companyIndustry,
  P.companyId as parentCompanyId,
  P.name as parentCompanyName,
  N.companyId as networkCompanyId,
  N.name as networkCompanyName,
  N.companyIndustry as networkCompanyIndustry,
  CO.dailyDirectCost,
  CO.dailyIndirectCost
from costs CO
left outer join productionCompanies C on CO.companyId = C.companyId
left outer join productionCompanies P on C.parentId = P.companyId
left outer join networkCompanies N on C.companyId = N.subCompanyId 
left outer join playerVersions V on CO.endpointId = V.displayId
left outer join rise-core-log.coreData.licensedDisplays L on CO.endpointId = L.displayId
