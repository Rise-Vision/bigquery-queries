#standardSQL
  CREATE TABLE Uptime_Events.events (ts Timestamp not null,
    display_id String NOT NULL,
    showing boolean,
    connected boolean,
    scheduled boolean)
PARTITION BY
  date(ts)