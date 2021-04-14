CREATE TEMP FUNCTION URLDECODE(url STRING) AS ((
  SELECT STRING_AGG(
    IF(REGEXP_CONTAINS(y, r'^%[0-9a-fA-F]{2}'), 
      SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX(REPLACE(y, '%', ''))), y), '' 
    ORDER BY i
    )
  FROM UNNEST(REGEXP_EXTRACT_ALL(url, r"%[0-9a-fA-F]{2}(?:%[0-9a-fA-F]{2})*|[^%]+")) y
  WITH OFFSET AS i 
));

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
  D.displayId,
  D.companyId
from `rise-core-log.coreData.displays` D
inner join (select max(id) as id, displayId from `rise-core-log.coreData.displays` group by displayId) DD on D.id = DD.id
where D.appId = 's~rvaserver2'
group by 1, 2
),

productionSchedules as
(
select 
  S.scheduleId,
  S.companyId
from `rise-core-log.coreData.schedules` S
inner join (select max(id) as id, scheduleId from `rise-core-log.coreData.schedules` group by scheduleId) SS on S.id = SS.id
where S.appId = 's~rvaserver2'
group by 1, 2
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
  trim(C.display_id) as displayId,
  concat(trim(player_name), ' ', trim(player_version))  as playerVersion
from `client-side-events.Player_Data.configuration` C
inner join (select max(ts) as ts, display_id from `client-side-events.Player_Data.configuration` where DATE(ts) <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) and trim(display_id) != '' group by 2) CC on C.ts = CC.ts and C.display_id = CC.display_id
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

logInserts as
(
select count(*) as totalLogInsertCount
from `endpoint-event-logs.logs.eventLog`
where DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),

endpointLogInserts as
(
select 
  endpointId,
  endpointType,
  scheduleId,
  count(*) as logInsertCount
from `endpoint-event-logs.logs.eventLog`
where DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
group by 1, 2, 3
),

heartbeatInserts as
(
select count(*) as totalHeartbeatInsertCount
from `endpoint-event-logs.heartbeats.uptimeHeartbeats`
where DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),

endpointHeartbeatInserts as
(
select 
  endpointId,
  endpointType,
  scheduleId,
  count(*) as heatbeatInsertCount
from `endpoint-event-logs.heartbeats.uptimeHeartbeats`
where DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
group by 1, 2, 3
),

endpoints as
(
select 
  endpointId,
  endpointType,
  scheduleId
from endpointDownloads
union distinct
select 
  endpointId,
  endpointType,
  scheduleId
from endpointLogInserts
union distinct
select 
  endpointId,
  endpointType,
  scheduleId
from endpointHeartbeatInserts
),

endpointDetails as
(
select 
  H.endpointId,
  H.browserVersion,
  H.osVersion,
  H.eventAppVersion as viewerVersion
from `endpoint-event-logs.heartbeats.uptimeHeartbeats` H
inner join (select max(timestamp) as timestamp, endpointId from `endpoint-event-logs.heartbeats.uptimeHeartbeats` where DATE(timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)  group by 2) HH on H.timestamp = HH.timestamp and H.endpointId = HH.endpointId
where DATE(H.timestamp) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) and eventApp = 'Viewer'
group by 1, 2, 3, 4
),

endpointDownloads_legacy_display as
(
select 
REGEXP_EXTRACT(URLDECODE(REGEXP_EXTRACT(cs_uri, r'parent=([^?&#]*)')), r'id=([^?&#]*)') as endpointId,
'Display' as endpointType,
'' as scheduleId,
sum(sc_bytes) as downloadedBytes
from `avid-life-623.RiseStorageLogs_v2.UsageLogs*`
where _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d",DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) and 
(strpos(URLDECODE(cs_uri), '://widgets.risevision.com/viewer') > 0 or strpos(URLDECODE(cs_uri), '://viewer.risevision.com') > 0) and strpos(URLDECODE(cs_uri), 'ype=sharedschedule') <= 0
group by 1, 2, 3
),

endpointDownloads_legacy_display_referer as
(
select 
REGEXP_EXTRACT(URLDECODE(REGEXP_EXTRACT(cs_referer, r'parent=([^?&#]*)')), r'id=([^?&#]*)') as endpointId,
'Display' as endpointType,
'' as scheduleId,
sum(sc_bytes) as downloadedBytes
from `avid-life-623.RiseStorageLogs_v2.UsageLogs*`
where _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d",DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) and 
(strpos(URLDECODE(cs_referer), '://widgets.risevision.com/viewer') > 0 or strpos(URLDECODE(cs_referer), '://viewer.risevision.com') > 0) and strpos(URLDECODE(cs_referer), 'ype=sharedschedule') <= 0
group by 1, 2, 3
),

endpointDownloads_legacy_sharedschedule as
(
select 
REGEXP_EXTRACT(cs_uri, r'viewerId=([^?&#]*)') as endpointId,
case
 when lower(REGEXP_EXTRACT(cs_uri, r'env=([^?&#]*)')) = 'embed' or lower(REGEXP_EXTRACT(cs_uri, r'viewerEnv=([^?&#]*)')) = 'embed' then 'Embed'
 when lower(REGEXP_EXTRACT(cs_uri, r'env=([^?&#]*)')) = 'extension' or lower(REGEXP_EXTRACT(cs_uri, r'viewerEnv=([^?&#]*)')) = 'extension' then 'Extension'
 when strpos(lower(REGEXP_EXTRACT(cs_uri, r'env=([^?&#]*)')), 'apps_') > 0 or strpos(lower(REGEXP_EXTRACT(cs_uri, r'viewerEnv=([^?&#]*)')), 'apps_') > 0 then 'InApp'
 else 'URL'
end as endpointType,
REGEXP_EXTRACT(URLDECODE(REGEXP_EXTRACT(cs_uri, r'parent=([^?&#]*)')), r'id=([^?&#]*)') as scheduleId,
sum(sc_bytes) as downloadedBytes
from `avid-life-623.RiseStorageLogs_v2.UsageLogs*`
where _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d",DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) and strpos(URLDECODE(cs_uri), 'ype=sharedschedule') > 0
group by 1, 2, 3
),

endpointDownloads_legacy_sharedschedule_referer as
(
select 
REGEXP_EXTRACT(cs_referer, r'viewerId=([^?&#]*)') as endpointId,
case
 when lower(REGEXP_EXTRACT(cs_referer, r'env=([^?&#]*)')) = 'embed' or lower(REGEXP_EXTRACT(cs_referer, r'viewerEnv=([^?&#]*)')) = 'embed' then 'Embed'
 when lower(REGEXP_EXTRACT(cs_referer, r'env=([^?&#]*)')) = 'extension' or lower(REGEXP_EXTRACT(cs_referer, r'viewerEnv=([^?&#]*)')) = 'extension' then 'Extension'
 when strpos(lower(REGEXP_EXTRACT(cs_referer, r'env=([^?&#]*)')), 'apps_') > 0 or strpos(lower(REGEXP_EXTRACT(cs_referer, r'viewerEnv=([^?&#]*)')), 'apps_') > 0 then 'InApp'
 else 'URL'
end as endpointType,
REGEXP_EXTRACT(URLDECODE(REGEXP_EXTRACT(cs_referer, r'parent=([^?&#]*)')), r'id=([^?&#]*)') as scheduleId,
sum(sc_bytes) as downloadedBytes
from `avid-life-623.RiseStorageLogs_v2.UsageLogs*`
where _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d",DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) and strpos(URLDECODE(cs_referer), 'ype=sharedschedule') > 0
group by 1, 2, 3
),

endpointDownloads_legacy_display_signedUrls as
(
select 
REGEXP_EXTRACT(cs_uri, r'displayId=([^?&#]*)') as endpointId,
'Display' as endpointType,
'' as scheduleId,
sum(sc_bytes) as downloadedBytes
from `avid-life-623.RiseStorageLogs_v2.UsageLogs*`
where _TABLE_SUFFIX = FORMAT_DATE("%Y%m%d",DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) 
and strpos(cs_uri, 'GoogleAccessId=') > 0 and strpos(cs_uri, 'Signature=') > 0 and strpos(cs_uri, 'Expires=') > 0
group by 1, 2, 3
),

endpointDownloads_legacy as
(
select 
  endpointId,
  endpointType,
  scheduleId,
  downloadedBytes
from endpointDownloads_legacy_display
union distinct
select 
  endpointId,
  endpointType,
  scheduleId,
  downloadedBytes
from endpointDownloads_legacy_display_referer
union distinct
select 
  endpointId,
  endpointType,
  scheduleId,
  downloadedBytes
from endpointDownloads_legacy_sharedschedule
union distinct
select 
  endpointId,
  endpointType,
  scheduleId,
  downloadedBytes
from endpointDownloads_legacy_sharedschedule_referer
union distinct
select 
  endpointId,
  endpointType,
  scheduleId,
  downloadedBytes
from endpointDownloads_legacy_display_signedUrls
),

costs as
(
select
  DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) as date,
  E.*,
  case E.endpointType 
    when 'Display' then D.companyId
    else S.companyId
  end as companyId,
  IFNULL((CAST(B.downloadedBytes as FLOAT64) * CAST(totalDownloadCost as FLOAT64)) / CAST(totalDownloaded as FLOAT64), 0.0) as dailyDirectCost,
  IFNULL((CAST(L.downloadedBytes as FLOAT64) * CAST(totalDownloadCost as FLOAT64)) / CAST(totalDownloaded as FLOAT64), 0.0) as dailyDirectCost_legacy,
  IFNULL((CAST((IFNULL(logInsertCount, 0) +  IFNULL(heatbeatInsertCount, 0)) as FLOAT64) * CAST(totalStreamInsertCost as FLOAT64)) / CAST((totalLogInsertCount + totalHeartbeatInsertCount) as FLOAT64), 0.0) as dailyIndirectCost
from endpoints E, downloadBandwidth, downloadCost, streamInsertCost, logInserts, heartbeatInserts
left outer join endpointDownloads B on E.endpointId = B.endpointId and E.endpointType = B.endpointType and E.scheduleId = B.scheduleId
left outer join endpointDownloads_legacy L on E.endpointId = L.endpointId
left outer join endpointLogInserts LI on E.endpointId = LI.endpointId and E.endpointType = LI.endpointType and E.scheduleId = LI.scheduleId
left outer join endpointHeartbeatInserts HI on E.endpointId = HI.endpointId and E.endpointType = HI.endpointType and E.scheduleId = HI.scheduleId
left outer join productionDisplays D on E.endpointId = D.displayId
left outer join productionSchedules S on E.scheduleId = S.scheduleId
)

select 
  CO.date,
  CO.endpointId,
  CO.endpointType,
  if(L.displayId is null, 'Unlicensed', if(L.planSubscriptionStatus <> 'Suspended', 'Licensed', 'Suspended')) as licenseStatus,
  D.browserVersion,
  V.playerVersion,
  D.viewerVersion,
  D.osVersion,
  CO.scheduleId,
  CO.companyId,
  C.name as companyName,
  C.companyIndustry,
  P.companyId as parentCompanyId,
  P.name as parentCompanyName,
  N.companyId as networkCompanyId,
  N.name as networkCompanyName,
  N.companyIndustry as networkCompanyIndustry,
  CO.dailyDirectCost + CO.dailyDirectCost_legacy as dailyDirectCost,
  CO.dailyIndirectCost
from costs CO
left outer join endpointDetails D on CO.endpointId = D.endpointId
left outer join productionCompanies C on CO.companyId = C.companyId
left outer join productionCompanies P on C.parentId = P.companyId
left outer join networkCompanies N on C.companyId = N.subCompanyId 
left outer join playerVersions V on CO.endpointId = V.displayId
left outer join rise-core-log.coreData.licensedDisplays L on CO.endpointId = L.displayId
