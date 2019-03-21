#StandardSQL

SELECT template, COUNT( DISTINCT display_id ) as number_of_displays
FROM `client-side-events.Aggregate_Tables.DisplaysUsingHtmlTemplates`
WHERE date > DATE_ADD(CURRENT_DATE(), INTERVAL -30 DAY)
GROUP BY template
ORDER BY template
