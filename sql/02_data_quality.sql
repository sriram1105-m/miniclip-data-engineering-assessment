
-- 1. Check missing required fields by event type.
-- Each event type has different required fields.

SELECT
    `event-type` AS event_type,
    COUNT(*) AS total_events,
    SUM(
        CASE
            WHEN `time` IS NULL
              OR (
                  `event-type` = 'init'
                  AND (
                      `user-id` IS NULL
                      OR country IS NULL
                      OR platform IS NULL
                  )
              )
              OR (
                  `event-type` = 'match'
                  AND (
                      `user-a` IS NULL
                      OR `user-b` IS NULL
                      OR winner IS NULL
                  )
              )
              OR (
                  `event-type` = 'in-app-purchase'
                  AND (
                      `user-id` IS NULL
                      OR `product-id` IS NULL
                      OR purchase_value IS NULL
                  )
              )
            THEN 1
            ELSE 0
        END
    ) AS records_with_missing_fields
FROM raw_events
GROUP BY `event-type`
ORDER BY event_type;

-- 2. Identify exact duplicate records.

SELECT
    COUNT(*) AS total_events,
    COUNT(DISTINCT TO_JSON(STRUCT(*))) AS distinct_records,
    COUNT(*) - COUNT(DISTINCT TO_JSON(STRUCT(*))) AS exact_duplicates
FROM raw_events;

-- 3. Check invalid purchase amounts.

SELECT
    COUNT(*) AS total_purchases,
    SUM(
        CASE
            WHEN purchase_value IS NULL
              OR ISNAN(purchase_value)
              OR purchase_value <= 0
            THEN 1
            ELSE 0
        END
    ) AS invalid_purchase_amounts
FROM raw_events
WHERE `event-type` = 'in-app-purchase';

-- 4. Check whether referenced players have an Init event.

WITH init_users AS (
    SELECT DISTINCT `user-id` AS user_id
    FROM raw_events
    WHERE `event-type` = 'init'
),
referenced_users AS (
    SELECT `event-type` AS event_type, `user-id` AS user_id
    FROM raw_events
    WHERE `event-type` = 'in-app-purchase'

    UNION ALL

    SELECT 'match', `user-a`
    FROM raw_events
    WHERE `event-type` = 'match'

    UNION ALL

    SELECT 'match', `user-b`
    FROM raw_events
    WHERE `event-type` = 'match'
)
SELECT
    r.event_type,
    COUNT(*) AS player_references,
    SUM(
        CASE WHEN i.user_id IS NULL THEN 1 ELSE 0 END
    ) AS references_without_init
FROM referenced_users r
LEFT JOIN init_users i
    ON r.user_id = i.user_id
GROUP BY r.event_type
ORDER BY r.event_type;
