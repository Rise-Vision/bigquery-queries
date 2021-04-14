CREATE OR REPLACE FUNCTION `endpoint-event-logs.logs.urlDecode`(url STRING) AS ((
  SELECT STRING_AGG(
    IF(REGEXP_CONTAINS(y, r'^%[0-9a-fA-F]{2}'), 
      SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX(REPLACE(y, '%', ''))), y), '' 
    ORDER BY i
    )
  FROM UNNEST(REGEXP_EXTRACT_ALL(url, r"%[0-9a-fA-F]{2}(?:%[0-9a-fA-F]{2})*|[^%]+")) y
  WITH OFFSET AS i 
));