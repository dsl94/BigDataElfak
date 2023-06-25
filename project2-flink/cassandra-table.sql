create table if not exists flink.stats
(
    time         timestamp primary key,
    avg_duration float,
    max_duration float,
    min_duration float,
    num_rides1   int,
    num_rides2   int,
    num_rides3   int,
    station1     text,
    station2     text,
    station3     text
);
