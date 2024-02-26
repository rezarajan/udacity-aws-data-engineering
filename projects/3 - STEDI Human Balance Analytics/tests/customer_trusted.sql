-- Returns the row count and null count of shared timestamps
-- Assert that row_count = 482, null_shared_count = 0
SELECT
    COUNT(*) AS row_count,
    SUM(
        CASE WHEN sharewithresearchasofdate IS NULL THEN 1
        ELSE 0 END
    ) AS null_shared_count
FROM customer_trusted;