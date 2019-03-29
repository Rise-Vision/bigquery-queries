#StandardSQL

SELECT * FROM
(
  SELECT
    DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) as date,
    template,
    display_id,
    company_id
  FROM
  (
    SELECT display_id, company_id, template.name AS template
    FROM `client-side-events.Display_Events.events`
    WHERE ts >= TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY))
    AND ts < TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL -0 DAY))
    AND platform = 'content'
    AND rollout_stage IN( 'beta', 'stable' )
    AND template.name IS NOT NULL
    AND template.name != ''
    GROUP BY 1, 2, 3
  ) AS total

  UNION ALL

  SELECT date, template, display_id, company_id
  FROM `client-side-events.Aggregate_Tables.DisplaysUsingHtmlTemplates`
  WHERE date < DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY)
)
ORDER BY date DESC, template
