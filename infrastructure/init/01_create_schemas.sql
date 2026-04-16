-- Run after `docker compose up -d` once Trino is healthy:
--   docker exec -it trino trino --execute "$(cat infrastructure/init/01_create_schemas.sql)"
--
-- Creates the target and source schemas with explicit S3 locations so Iceberg
-- writes land in the correct MinIO bucket.

CREATE SCHEMA IF NOT EXISTS lz_lakehouse.lm_target_schema
    WITH (location = 's3a://lz-lakehouse/lm_target_schema');

CREATE SCHEMA IF NOT EXISTS oracle_homeloans.public
    WITH (location = 's3a://oracle-homeloans/public');

-- Verify
SHOW SCHEMAS FROM lz_lakehouse;
SHOW SCHEMAS FROM oracle_homeloans;
