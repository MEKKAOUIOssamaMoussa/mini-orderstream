-- Runs once, when the Postgres container starts with an empty data directory.
-- Tables are added in stage 2.
CREATE DATABASE orders_db;
CREATE DATABASE analytics_db;
