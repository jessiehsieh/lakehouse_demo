CREATE TABLE IF NOT EXISTS nessie.interactions_PartBy_date(
        userid             int,
        itemid             int,
        event             VARCHAR(50),
        event_date             date,
        event_datetime        timestamp)
PARTITION BY (event_date);

INSERT INTO nessie.interactions_PartBy_date
SELECT 
    cast(tab."user" as INTEGER) as userid, 
    cast("item" as INTEGER) as itemid, 
    "event",
    cast(from_unixtime(cast("timestamp" as INTEGER)) as date) as "event_date",
    cast(from_unixtime(cast("timestamp" as INTEGER)) as timestamp) as "event_timestamp"
FROM nessie.interactions as tab
ORDER BY "event_date";