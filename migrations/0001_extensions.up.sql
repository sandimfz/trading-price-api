DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'timescaledb' AND installed_version IS NOT NULL) THEN
        CREATE EXTENSION timescaledb;
    END IF;
END $$;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";