-- Polaris uses the same PostgreSQL instance as the operational source, but a
-- separate database and schema. This script runs only on first initialisation
-- of an empty postgres_data volume.
CREATE DATABASE polaris;

\connect polaris

-- Polaris does not create its schema itself; it selects it through the JDBC
-- driver's currentSchema property (default POLARIS_SCHEMA).
CREATE SCHEMA IF NOT EXISTS polaris_schema;
