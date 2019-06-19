#standardSQL
  CREATE TABLE Template_Uptime_Events.events (ts Timestamp not null,
    display_id String NOT NULL,
    presentation_id String NOT NULL,
    template_product_code String NOT NULL,
    template_version String NOT NULL,
    responding boolean,
    error boolean)
PARTITION BY
  date(ts)