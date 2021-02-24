CREATE OR REPLACE FUNCTION `endpoint-event-logs.logs.cleanUpEventApp`(eventApp STRING) AS (
if(eventApp like '%: % ID: %', REGEXP_REPLACE(eventApp, r":\s.+\sID: ", ": "), eventApp)
);