
-- 1. Count records by event type
SELECT
    `event-type` AS event_type,
    COUNT(*) AS total_events
FROM raw_events
GROUP BY `event-type`
ORDER BY event_type;

-- 2. Count distinct users by country and platform
SELECT
    country,
    platform,
    COUNT(DISTINCT `user-id`) AS distinct_users
FROM raw_events
WHERE `event-type` = 'init'
GROUP BY country, platform
ORDER BY country, platform;
