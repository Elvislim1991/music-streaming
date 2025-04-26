-- Validation Queries for Music Streaming Analytics

-- 1. Basic validation: Count records in each table
SELECT 'users' as table_name, COUNT(*) as record_count FROM users
UNION ALL
SELECT 'artists', COUNT(*) FROM artists
UNION ALL
SELECT 'albums', COUNT(*) FROM albums
UNION ALL
SELECT 'songs', COUNT(*) FROM songs
UNION ALL
SELECT 'devices', COUNT(*) FROM devices
UNION ALL
SELECT 'locations', COUNT(*) FROM locations
UNION ALL
SELECT 'stream_events', COUNT(*) FROM stream_events
UNION ALL
SELECT 'user_engagement_metrics', COUNT(*) FROM user_engagement_metrics
UNION ALL
SELECT 'content_performance_metrics', COUNT(*) FROM content_performance_metrics
UNION ALL
SELECT 'device_metrics', COUNT(*) FROM device_metrics
UNION ALL
SELECT 'location_metrics', COUNT(*) FROM location_metrics
UNION ALL
SELECT 'hourly_metrics', COUNT(*) FROM hourly_metrics;

-- 2. Validate referential integrity
-- Check for orphaned records in stream_events
SELECT 'Orphaned user_id' as issue, COUNT(*) as count
FROM stream_events se
LEFT JOIN users u ON se.user_id = u.user_id
WHERE u.user_id IS NULL AND se.user_id IS NOT NULL
UNION ALL
SELECT 'Orphaned song_id', COUNT(*)
FROM stream_events se
LEFT JOIN songs s ON se.song_id = s.song_id
WHERE s.song_id IS NULL AND se.song_id IS NOT NULL
UNION ALL
SELECT 'Orphaned artist_id', COUNT(*)
FROM stream_events se
LEFT JOIN artists a ON se.artist_id = a.artist_id
WHERE a.artist_id IS NULL AND se.artist_id IS NOT NULL
UNION ALL
SELECT 'Orphaned album_id', COUNT(*)
FROM stream_events se
LEFT JOIN albums a ON se.album_id = a.album_id
WHERE a.album_id IS NULL AND se.album_id IS NOT NULL
UNION ALL
SELECT 'Orphaned device_id', COUNT(*)
FROM stream_events se
LEFT JOIN devices d ON se.device_id = d.device_id
WHERE d.device_id IS NULL AND se.device_id IS NOT NULL
UNION ALL
SELECT 'Orphaned location_id', COUNT(*)
FROM stream_events se
LEFT JOIN locations l ON se.location_id = l.location_id
WHERE l.location_id IS NULL AND se.location_id IS NOT NULL;

-- 3. Business Questions

-- 3.1 Top 10 most streamed songs
SELECT s.title, a.name as artist_name, COUNT(*) as stream_count
FROM stream_events se
JOIN songs s ON se.song_id = s.song_id
JOIN artists a ON se.artist_id = a.artist_id
GROUP BY s.title, a.name
ORDER BY stream_count DESC
LIMIT 10;

-- 3.2 Top 10 most active users
SELECT u.username, u.subscription_type, COUNT(*) as stream_count, 
       SUM(se.stream_duration_seconds) as total_listen_time
FROM stream_events se
JOIN users u ON se.user_id = u.user_id
GROUP BY u.username, u.subscription_type
ORDER BY stream_count DESC
LIMIT 10;

-- 3.3 Streaming activity by hour of day
SELECT EXTRACT(HOUR FROM timestamp) as hour_of_day, 
       COUNT(*) as stream_count,
       COUNT(DISTINCT user_id) as unique_users
FROM stream_events
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- 3.4 Streaming activity by device type
SELECT d.device_type, d.os, COUNT(*) as stream_count,
       AVG(se.stream_duration_seconds) as avg_listen_time
FROM stream_events se
JOIN devices d ON se.device_id = d.device_id
GROUP BY d.device_type, d.os
ORDER BY stream_count DESC;

-- 3.5 Skip rate by genre
SELECT s.genre, 
       COUNT(*) as total_streams,
       SUM(CASE WHEN se.is_skipped THEN 1 ELSE 0 END) as skipped_streams,
       ROUND(SUM(CASE WHEN se.is_skipped THEN 1 ELSE 0 END)::numeric / COUNT(*)::numeric * 100, 2) as skip_rate
FROM stream_events se
JOIN songs s ON se.song_id = s.song_id
GROUP BY s.genre
ORDER BY skip_rate DESC;

-- 3.6 Like rate by genre
SELECT s.genre, 
       COUNT(*) as total_streams,
       SUM(CASE WHEN se.is_liked THEN 1 ELSE 0 END) as liked_streams,
       ROUND(SUM(CASE WHEN se.is_liked THEN 1 ELSE 0 END)::numeric / COUNT(*)::numeric * 100, 2) as like_rate
FROM stream_events se
JOIN songs s ON se.song_id = s.song_id
GROUP BY s.genre
ORDER BY like_rate DESC;

-- 3.7 Streaming patterns by country
SELECT l.country, COUNT(*) as stream_count, COUNT(DISTINCT se.user_id) as unique_users
FROM stream_events se
JOIN locations l ON se.location_id = l.location_id
GROUP BY l.country
ORDER BY stream_count DESC
LIMIT 20;

-- 3.8 Streaming patterns by subscription type
SELECT u.subscription_type, 
       COUNT(*) as stream_count,
       COUNT(DISTINCT se.user_id) as unique_users,
       AVG(se.stream_duration_seconds) as avg_listen_time,
       ROUND(SUM(CASE WHEN se.is_skipped THEN 1 ELSE 0 END)::numeric / COUNT(*)::numeric * 100, 2) as skip_rate,
       ROUND(SUM(CASE WHEN se.is_complete_play THEN 1 ELSE 0 END)::numeric / COUNT(*)::numeric * 100, 2) as completion_rate
FROM stream_events se
JOIN users u ON se.user_id = u.user_id
GROUP BY u.subscription_type
ORDER BY stream_count DESC;

-- 3.9 Trending songs (last 24 hours vs previous 24 hours)
WITH last_24h AS (
    SELECT song_id, COUNT(*) as streams
    FROM stream_events
    WHERE timestamp >= NOW() - INTERVAL '24 hours'
    GROUP BY song_id
),
previous_24h AS (
    SELECT song_id, COUNT(*) as streams
    FROM stream_events
    WHERE timestamp >= NOW() - INTERVAL '48 hours' AND timestamp < NOW() - INTERVAL '24 hours'
    GROUP BY song_id
)
SELECT s.title, a.name as artist_name, 
       COALESCE(l.streams, 0) as last_24h_streams,
       COALESCE(p.streams, 0) as previous_24h_streams,
       CASE 
           WHEN COALESCE(p.streams, 0) = 0 THEN 100
           ELSE ROUND((COALESCE(l.streams, 0) - COALESCE(p.streams, 0))::numeric / COALESCE(p.streams, 1)::numeric * 100, 2)
       END as growth_percent
FROM last_24h l
FULL OUTER JOIN previous_24h p ON l.song_id = p.song_id
JOIN songs s ON COALESCE(l.song_id, p.song_id) = s.song_id
JOIN stream_events se ON s.song_id = se.song_id
JOIN artists a ON se.artist_id = a.artist_id
GROUP BY s.title, a.name, l.streams, p.streams
ORDER BY growth_percent DESC
LIMIT 20;

-- 3.10 User engagement metrics by age group
SELECT 
    CASE 
        WHEN age < 18 THEN 'Under 18'
        WHEN age BETWEEN 18 AND 24 THEN '18-24'
        WHEN age BETWEEN 25 AND 34 THEN '25-34'
        WHEN age BETWEEN 35 AND 44 THEN '35-44'
        WHEN age BETWEEN 45 AND 54 THEN '45-54'
        ELSE '55+'
    END as age_group,
    COUNT(DISTINCT se.user_id) as unique_users,
    COUNT(*) as stream_count,
    ROUND(COUNT(*)::numeric / COUNT(DISTINCT se.user_id)::numeric, 2) as streams_per_user,
    ROUND(AVG(se.stream_duration_seconds), 2) as avg_listen_time,
    ROUND(SUM(CASE WHEN se.is_skipped THEN 1 ELSE 0 END)::numeric / COUNT(*)::numeric * 100, 2) as skip_rate,
    ROUND(SUM(CASE WHEN se.is_liked THEN 1 ELSE 0 END)::numeric / COUNT(*)::numeric * 100, 2) as like_rate,
    ROUND(SUM(CASE WHEN se.is_shared THEN 1 ELSE 0 END)::numeric / COUNT(*)::numeric * 100, 2) as share_rate
FROM stream_events se
JOIN users u ON se.user_id = u.user_id
GROUP BY age_group
ORDER BY age_group;

-- 3.11 Validate metrics tables against raw data
-- Compare user engagement metrics with calculated values from raw data
SELECT 
    uem.user_id,
    uem.stream_count as metrics_stream_count,
    COUNT(*) as calculated_stream_count,
    ABS(uem.stream_count - COUNT(*)) as count_difference
FROM user_engagement_metrics uem
JOIN stream_events se ON uem.user_id = se.user_id
WHERE se.timestamp BETWEEN uem.time_window AND uem.time_window + INTERVAL '5 minutes'
GROUP BY uem.user_id, uem.time_window, uem.stream_count
HAVING ABS(uem.stream_count - COUNT(*)) > 0
LIMIT 10;

-- 3.12 Completeness check for dimension data
SELECT 
    (SELECT COUNT(DISTINCT user_id) FROM stream_events) as unique_users_in_events,
    (SELECT COUNT(*) FROM users) as users_in_dimension,
    (SELECT COUNT(DISTINCT song_id) FROM stream_events) as unique_songs_in_events,
    (SELECT COUNT(*) FROM songs) as songs_in_dimension,
    (SELECT COUNT(DISTINCT artist_id) FROM stream_events) as unique_artists_in_events,
    (SELECT COUNT(*) FROM artists) as artists_in_dimension,
    (SELECT COUNT(DISTINCT device_id) FROM stream_events) as unique_devices_in_events,
    (SELECT COUNT(*) FROM devices) as devices_in_dimension,
    (SELECT COUNT(DISTINCT location_id) FROM stream_events) as unique_locations_in_events,
    (SELECT COUNT(*) FROM locations) as locations_in_dimension;