CREATE TABLE IF NOT EXISTS service_metadata (
    service_id   VARCHAR(128) PRIMARY KEY,
    service_name VARCHAR(256) NOT NULL,
    team         VARCHAR(128),
    environment  VARCHAR(64),
    region       VARCHAR(64)
);

INSERT INTO service_metadata VALUES
  ('svc-api',     'API Service',     'platform', 'production', 'us-east-1'),
  ('svc-worker',  'Worker Service',  'data',     'production', 'us-east-1'),
  ('svc-unknown', 'Unknown Service', 'unknown',  'unknown',    'unknown')
ON CONFLICT (service_id) DO NOTHING;
