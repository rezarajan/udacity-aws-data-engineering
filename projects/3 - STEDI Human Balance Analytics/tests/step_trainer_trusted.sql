-- Returns the row count
-- Assert that row_count = 14460
SELECT
    COUNT(*) AS row_count,
    COUNT(*) - SUM(piiexclude) AS pii_timestamp_exclude_count
FROM step_trainer_trusted;