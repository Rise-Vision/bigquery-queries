CREATE OR REPLACE FUNCTION `endpoint-event-logs.logs.cleanUpEventApp`(eventApp STRING) AS (
if(eventApp like '%: sc%_pre%_%_% ID: %', REGEXP_REPLACE(REGEXP_REPLACE(eventApp, r"_\d+[wigt]\sID: ([a-zA-Z0-9\-\s]+$)", ""), r"sc\d+_pre\d+_", ""), eventApp)
);