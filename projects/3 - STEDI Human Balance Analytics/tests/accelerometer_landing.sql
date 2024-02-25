-- Returns the row count and pii-filtered count based on opt-in timestamp
-- Assert that row_count = 40981, pii_timestamp_exclude_count = 32025
SELECT
    COUNT(*) AS row_count,
    COUNT(*) - SUM(piiexclude) AS pii_timestamp_exclude_count
FROM accelerometer_trusted;