with

productionDisplays as
(
select 
  D.*
from `rise-core-log.coreData.displays` D
inner join (select max(id) as id, displayId from `rise-core-log.coreData.displays` group by displayId) DD on D.id = DD.id
where D.appId = 's~rvaserver2'
)

select
  companyId,
  displayId,
  licensedBy,
  planSubscriptionStatus
from (
select
  S.companyId,
  displayId,
  S.companyId as licensedBy,
  S.planSubscriptionStatus
from `rise-core-log.coreData.companySubscriptions` S, S.playerProAssignedDisplays as displayId
inner join (select max(id) as id, companyId from `rise-core-log.coreData.companySubscriptions` group by companyId) SS on S.id = SS.id
where
  (S.planCompanyId is null or S.planCompanyId = "") and
  `rise-core-log.coreData.arrayIndexOf`(S.playerProAssignedDisplays, displayId) < `rise-core-log.coreData.getTotalLicenses`(S.planSubscriptionStatus, S.planCurrentPeriodEndDate, S.planTrialExpiryDate, S.playerProSubscriptionStatus, S.playerProCurrentPeriodEndDate, S.planPlayerProLicenseCount, S.playerProLicenseCount)
union distinct
select
  SC.companyId,
  displayId,
  S.companyId as licensedBy,
  S.planSubscriptionStatus
from `rise-core-log.coreData.companySubscriptions` S, S.playerProTotalAssignedDisplays as displayId
inner join (select max(id) as id, companyId from `rise-core-log.coreData.companySubscriptions` group by companyId) SS on S.id = SS.id
inner join `rise-core-log.coreData.companySubscriptions` SC on S.companyId = SC.planCompanyId
inner join (select max(id) as id, companyId from `rise-core-log.coreData.companySubscriptions` group by companyId) SSS on SC.id = SSS.id
inner join productionDisplays D on SC.companyId = D.companyId and displayId = D.displayId
where
  `rise-core-log.coreData.arrayIndexOf`(S.playerProTotalAssignedDisplays, displayId) < `rise-core-log.coreData.getTotalLicenses`(S.planSubscriptionStatus, S.planCurrentPeriodEndDate, S.planTrialExpiryDate, S.playerProSubscriptionStatus, S.playerProCurrentPeriodEndDate, S.planPlayerProLicenseCount, S.playerProLicenseCount)
)
