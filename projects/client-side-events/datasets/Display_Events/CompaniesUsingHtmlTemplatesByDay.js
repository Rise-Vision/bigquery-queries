#StandardSQL

SELECT date, template, COUNT( DISTINCT company_id ) as number_of_companies
FROM `client-side-events.Aggregate_Tables.DisplaysUsingHtmlTemplates`
WHERE date > DATE_ADD(CURRENT_DATE(), INTERVAL -30 DAY)
GROUP BY date, template
ORDER BY date DESC, template
