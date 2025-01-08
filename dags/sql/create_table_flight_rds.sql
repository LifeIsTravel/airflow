CREATE TABLE flight
(
    flight_id              SERIAL PRIMARY KEY, -- 자동 증가하는 PRIMARY KEY
    extracted_at           TEXT,
    departure_date         TEXT,
    departure_display_code VARCHAR(10),
    departure_name         VARCHAR(100),
    arrival_display_code   VARCHAR(10),
    arrival_name           VARCHAR(100),
    carrier_names          TEXT[],             -- 배열 형태
    departure_time         TEXT,
    arrival_time           TEXT,
    agent_name             VARCHAR(100),
    amount                 DOUBLE PRECISION,
    url                    TEXT,               -- URL 문자열
    last_updated           TEXT,
    stop_count             TEXT
);
