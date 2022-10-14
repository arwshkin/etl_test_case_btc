CREATE TABLE IF NOT EXISTS currencies_staging (
    ticker_name varchar(10),
    ticker_timestamp timestamp,
    ticker_value double precision
);

CREATE TABLE IF NOT EXISTS currencies (
    ticker_name varchar(10),
    ticker_timestamp timestamp,
    ticker_value double precision,
    CONSTRAINT currencies_unique UNIQUE (ticker_name, ticker_timestamp)
);