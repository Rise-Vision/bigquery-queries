#standardSQL
  CREATE TABLE ChromeOS_Player_Events.events (event String NOT NULL,
    event_details String,
    id String NOT NULL,
    ip String,
    os String,
    chrome_version String,
    player_version String,
    ts Timestamp not null)
PARTITION BY
  date(ts)