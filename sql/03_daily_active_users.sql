
-- Daily distinct users by country and platform
-- Based on Init events
-- Unix timestamps are interpreted in UTC

SELECT
    TO_DATE(FROM_UNIXTIME(`time`)) AS event_date,
    country,
    platform,
    COUNT(DISTINCT `user-id`) AS daily_active_users
FROM raw_events
WHERE `event-type` = 'init'
  AND `user-id` IS NOT NULL
  AND `time` IS NOT NULL
  AND country IS NOT NULL
  AND platform IS NOT NULL
GROUP BY
    TO_DATE(FROM_UNIXTIME(`time`)),
    country,
    platform
ORDER BY
    event_date,
    country,
    platform;
