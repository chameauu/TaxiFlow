-- Phase 4 CDC source. The source owns trip_id so updates and deletes have a key.
CREATE SCHEMA IF NOT EXISTS iot;

CREATE TABLE IF NOT EXISTS iot.taxi_nyc_time_series (
    trip_id text PRIMARY KEY,
    vendor_id integer NOT NULL,
    pickup_datetime timestamp NOT NULL,
    dropoff_datetime timestamp NOT NULL,
    passenger_count integer,
    trip_distance numeric(10, 2),
    fare_amount numeric(12, 2),
    tip_amount numeric(12, 2),
    total_amount numeric(12, 2),
    payment_type integer,
    pickup_zone_id integer,
    dropoff_zone_id integer,
    service text NOT NULL DEFAULT 'yellow',
    updated_at timestamp NOT NULL DEFAULT now()
);

-- Deletes must carry every column, not just the primary key, so the stream can
-- retain a latest-state tombstone instead of losing the row payload.
ALTER TABLE iot.taxi_nyc_time_series REPLICA IDENTITY FULL;

CREATE OR REPLACE FUNCTION iot.seed_synthetic_trips(p_count integer DEFAULT 3)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    i integer;
BEGIN
    FOR i IN 1..p_count LOOP
        INSERT INTO iot.taxi_nyc_time_series (
            trip_id, vendor_id, pickup_datetime, dropoff_datetime,
            passenger_count, trip_distance, fare_amount, tip_amount,
            total_amount, payment_type, pickup_zone_id, dropoff_zone_id,
            service, updated_at
        ) VALUES (
            'cdc-' || lpad(i::text, 6, '0'),
            1,
            timestamp '2026-10-24 12:00:00' + (i * interval '1 minute'),
            timestamp '2026-10-24 12:20:00' + (i * interval '1 minute'),
            1 + (i % 3),
            1.25 + i,
            10.00 + i,
            1.00 + (i * 0.25),
            12.00 + (i * 1.25),
            1,
            100 + i,
            200 + i,
            'yellow',
            timestamp '2026-10-24 12:30:00'
        )
        ON CONFLICT (trip_id) DO NOTHING;
    END LOOP;
    RETURN p_count;
END;
$$;

CREATE OR REPLACE FUNCTION iot.emit_synthetic_trip()
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    new_id text;
    changed_id text;
BEGIN
    new_id := 'cdc-' || to_char(clock_timestamp(), 'YYYYMMDDHH24MISSMS')
        || '-' || lpad((floor(random() * 100000))::integer::text, 5, '0');

    INSERT INTO iot.taxi_nyc_time_series (
        trip_id, vendor_id, pickup_datetime, dropoff_datetime,
        passenger_count, trip_distance, fare_amount, tip_amount,
        total_amount, payment_type, pickup_zone_id, dropoff_zone_id,
        service, updated_at
    ) VALUES (
        new_id, 1, now()::timestamp, (now() + interval '20 minutes')::timestamp,
        1 + (floor(random() * 4))::integer,
        round((1.0 + random() * 10)::numeric, 2),
        round((8.0 + random() * 30)::numeric, 2),
        round((random() * 5)::numeric, 2),
        round((15.0 + random() * 35)::numeric, 2),
        1,
        100 + (floor(random() * 50))::integer,
        200 + (floor(random() * 50))::integer,
        'yellow',
        now()::timestamp
    );

    SELECT trip_id INTO changed_id
    FROM iot.taxi_nyc_time_series
    ORDER BY random()
    LIMIT 1;

    IF changed_id IS NOT NULL THEN
        UPDATE iot.taxi_nyc_time_series
        SET tip_amount = coalesce(tip_amount, 0) + 0.01,
            total_amount = coalesce(total_amount, 0) + 0.01,
            updated_at = now()::timestamp
        WHERE trip_id = changed_id;
    END IF;

    RETURN new_id;
END;
$$;

CREATE OR REPLACE FUNCTION iot.delete_synthetic_trip()
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    deleted_id text;
BEGIN
    DELETE FROM iot.taxi_nyc_time_series
    WHERE trip_id = (
        SELECT trip_id
        FROM iot.taxi_nyc_time_series
        ORDER BY random()
        LIMIT 1
    )
    RETURNING trip_id INTO deleted_id;
    RETURN deleted_id;
END;
$$;
