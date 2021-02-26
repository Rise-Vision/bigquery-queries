#standardSQL
with 

infrastuctureCosts as
(
select date, sum(dailyCost) as totalInfrastuctureCost
from `endpoint-event-logs.views.endpointInfrastructureCostsLast60Days` I
group by 1
),

infrastuctureCostPerEndpoint as
(
select I.date, I.totalInfrastuctureCost/count(distinct U.endpointId) as infrastuctureCost
from infrastuctureCosts I
inner join `endpoint-event-logs.logs.dailyUsage` U on I.date = U.date
where U.date between DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) and CURRENT_DATE() 
group by I.date, I.totalInfrastuctureCost 
)

select 
  C.date,
  endpointId, 
  endpointType, 
  licenseStatus, 
  browserVersion, 
  viewerVersion, 
  playerVersion, 
  osVersion, 
  scheduleId, 
  companyId, 
  companyName, 
  companyIndustry, 
  parentCompanyId, 
  parentCompanyName, 
  networkCompanyId, 
  networkCompanyName, 
  networkCompanyIndustry, 
  dailyDirectCost as downloadCost,
  dailyIndirectCost as loggingCost,
  infrastuctureCost
from `endpoint-event-logs.logs.dailyCost` C
inner join infrastuctureCostPerEndpoint I on I.date = C.date
where C.date between DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) and CURRENT_DATE() 
