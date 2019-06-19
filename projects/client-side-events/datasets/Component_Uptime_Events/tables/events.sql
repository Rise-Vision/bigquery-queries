#standardSQL
  CREATE TABLE Component_Uptime_Events.events (ts Timestamp not null,
    display_id String NOT NULL,
    presentation_id String NOT NULL,
    template_product_code String NOT NULL,
    template_version String NOT NULL,
    component_type String NOT NULL,
    component_id String NOT NULL,
    responding boolean,
    error boolean)
PARTITION BY
  date(ts)