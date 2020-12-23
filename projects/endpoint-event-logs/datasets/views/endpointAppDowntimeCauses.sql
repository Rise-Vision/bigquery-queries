#standardSQL
with

endpointErrors as 
(
select 
  date, 
  endpointId, 
  endpointType, 
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
  sum(errorCount) as errorCount
from `endpoint-event-logs.logs.dailyErrors` E
where  endpointType <> 'Unknown' and companyId is not null and (date between DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) and DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
group by 1, 2, 3,4 ,5 ,6 ,7, 8, 9, 10, 11, 12, 13, 14
)

select distinct
  date, 
  endpointId, 
  endpointType, 
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
  message,
  description,
  solution,
  errorCount
from endpointErrors E
inner join `endpoint-event-logs.errors.errorDefinitions` D on E.eventErrorCode = D.code
where isRelatedToUptime = 'Y'
order by date desc, errorCount desc