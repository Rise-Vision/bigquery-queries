#standardSQL
select 
  date(usage_end_time) as date,
  project.id as projectId,
  service.description as service,
  sku.description as SKU,
  sum(cost) as dailyCost 
from `rise-core-log.billingData.gcp_billing_export_v1_00FE29_4FDD82_EF884E` C
inner join (SELECT projectId FROM `endpoint-event-logs.views.infrastructureProjects` where isEndpointInfrastructure = 'Y') I on C.project.id = I.projectId
where
  (DATE(_PARTITIONTIME) between DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) and CURRENT_DATE()) and date(usage_end_time) <= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
group by 1, 2, 3, 4
order by 1 desc, 2, 3, 4