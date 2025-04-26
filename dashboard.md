# Real-Time Music Streaming Analytics Dashboard

## Introduction

This dashboard provides real-time visualizations of music streaming data, allowing you to monitor user engagement, content performance, device usage, and geographic patterns as they happen. The dashboard is designed to show how data changes over time, making it easy to identify trends, anomalies, and patterns in streaming behavior.

## Setup Instructions

### Prerequisites

- The music streaming analytics pipeline must be running (Kafka, Spark, PostgreSQL, and Superset)
- Apache Superset must be properly configured with a connection to the PostgreSQL database

### Accessing the Dashboard

1. Open your web browser and navigate to the Superset URL:
   - Local access: http://localhost:8088
   - Remote access: http://<your-ip-address>:8088

2. Log in with the following credentials:
   - Username: admin
   - Password: admin

3. Configure the PostgreSQL database connection (if not already done):
   - Go to Data → Databases → + Database
   - Set the SQLAlchemy URI to: `postgresql://postgres:postgres@postgres:5432/music_streaming`
   - Test the connection and save

4. Create datasets for each table:
   - Go to Data → Datasets → + Dataset
   - Select the PostgreSQL database you just configured
   - Create datasets for the following tables:
     - user_engagement_metrics
     - content_performance_metrics
     - device_metrics
     - location_metrics
     - hourly_metrics
     - stream_events (for raw event data)

5. Import the dashboard (if available) or create a new one:
   - Go to Dashboards → + Dashboard
   - Name it "Real-Time Music Streaming Analytics"
   - Start adding charts as described in the following sections

## Real-Time Visualizations

### 1. User Engagement Dashboard

This dashboard shows how users are engaging with the streaming service in real-time.

#### 1.1 Active Users Over Time

**Chart Type:** Line Chart

**SQL Query:**
```sql
SELECT 
    time_window,
    COUNT(DISTINCT user_id) AS active_users
FROM user_engagement_metrics
WHERE time_window >= NOW() - INTERVAL '1 hour'
GROUP BY time_window
ORDER BY time_window
```

**Interpretation:** This chart shows the number of active users over time. A steady increase indicates growing engagement, while sudden drops might indicate service issues.

#### 1.2 Average Listen Time by Subscription Type

**Chart Type:** Bar Chart

**SQL Query:**
```sql
SELECT 
    u.subscription_type,
    AVG(uem.avg_listen_time) AS average_listen_time
FROM user_engagement_metrics uem
JOIN users u ON uem.user_id = u.user_id
WHERE uem.time_window >= NOW() - INTERVAL '1 hour'
GROUP BY u.subscription_type
ORDER BY average_listen_time DESC
```

**Interpretation:** This chart compares the average listen time across different subscription types. Premium users typically have higher listen times than free users.

#### 1.3 Skip Rate Over Time

**Chart Type:** Line Chart

**SQL Query:**
```sql
SELECT 
    time_window,
    AVG(skip_rate) AS average_skip_rate
FROM user_engagement_metrics
WHERE time_window >= NOW() - INTERVAL '1 hour'
GROUP BY time_window
ORDER BY time_window
```

**Interpretation:** This chart shows how the skip rate changes over time. High skip rates might indicate content that doesn't match user preferences.

#### 1.4 User Engagement Heatmap

**Chart Type:** Heatmap

**SQL Query:**
```sql
SELECT 
    EXTRACT(HOUR FROM time_window) AS hour_of_day,
    user_id,
    SUM(stream_count) AS total_streams
FROM user_engagement_metrics
WHERE time_window >= NOW() - INTERVAL '24 hours'
GROUP BY EXTRACT(HOUR FROM time_window), user_id
```

**Interpretation:** This heatmap shows user activity patterns throughout the day, helping identify peak usage times and individual user behaviors.

### 2. Content Performance Dashboard

This dashboard tracks how songs and artists are performing in real-time.

#### 2.1 Top Songs by Stream Count

**Chart Type:** Bar Chart

**SQL Query:**
```sql
SELECT 
    s.title AS song_title,
    SUM(cpm.stream_count) AS total_streams
FROM content_performance_metrics cpm
JOIN songs s ON cpm.song_id = s.song_id
WHERE cpm.time_window >= NOW() - INTERVAL '1 hour'
GROUP BY s.title
ORDER BY total_streams DESC
LIMIT 10
```

**Interpretation:** This chart shows the most popular songs in the last hour based on stream count.

#### 2.2 Skip Rate by Genre

**Chart Type:** Bar Chart

**SQL Query:**
```sql
SELECT 
    s.genre,
    AVG(cpm.skip_rate) AS average_skip_rate
FROM content_performance_metrics cpm
JOIN songs s ON cpm.song_id = s.song_id
WHERE cpm.time_window >= NOW() - INTERVAL '1 hour'
GROUP BY s.genre
ORDER BY average_skip_rate DESC
```

**Interpretation:** This chart shows which genres have the highest skip rates, potentially indicating less engaging content.

#### 2.3 Like Rate Over Time

**Chart Type:** Line Chart

**SQL Query:**
```sql
SELECT 
    time_window,
    AVG(like_rate) AS average_like_rate
FROM content_performance_metrics
WHERE time_window >= NOW() - INTERVAL '1 hour'
GROUP BY time_window
ORDER BY time_window
```

**Interpretation:** This chart shows how the like rate changes over time, indicating overall content satisfaction.

#### 2.4 Artist Performance Comparison

**Chart Type:** Bar Chart

**SQL Query:**
```sql
SELECT 
    a.name AS artist_name,
    SUM(cpm.stream_count) AS total_streams,
    AVG(cpm.completion_rate) AS avg_completion_rate,
    AVG(cpm.like_rate) AS avg_like_rate
FROM content_performance_metrics cpm
JOIN artists a ON cpm.artist_id = a.artist_id
WHERE cpm.time_window >= NOW() - INTERVAL '1 hour'
GROUP BY a.name
ORDER BY total_streams DESC
LIMIT 10
```

**Interpretation:** This chart compares the performance of top artists based on streams, completion rates, and like rates.

### 3. Device Usage Dashboard

This dashboard shows how different devices are being used for streaming.

#### 3.1 Streams by Device Type

**Chart Type:** Pie Chart

**SQL Query:**
```sql
SELECT 
    d.device_type,
    SUM(dm.stream_count) AS total_streams
FROM device_metrics dm
JOIN devices d ON dm.device_id = d.device_id
WHERE dm.time_window >= NOW() - INTERVAL '1 hour'
GROUP BY d.device_type
```

**Interpretation:** This chart shows the distribution of streams across different device types, helping identify the most popular streaming platforms.

#### 3.2 Average Listen Time by Device Type

**Chart Type:** Bar Chart

**SQL Query:**
```sql
SELECT 
    d.device_type,
    AVG(dm.avg_listen_time) AS average_listen_time
FROM device_metrics dm
JOIN devices d ON dm.device_id = d.device_id
WHERE dm.time_window >= NOW() - INTERVAL '1 hour'
GROUP BY d.device_type
ORDER BY average_listen_time DESC
```

**Interpretation:** This chart shows which device types have the longest average listen times, indicating where users are most engaged.

#### 3.3 Completion Rate by OS

**Chart Type:** Bar Chart

**SQL Query:**
```sql
SELECT 
    d.os,
    AVG(dm.completion_rate) AS average_completion_rate
FROM device_metrics dm
JOIN devices d ON dm.device_id = d.device_id
WHERE dm.time_window >= NOW() - INTERVAL '1 hour'
GROUP BY d.os
ORDER BY average_completion_rate DESC
```

**Interpretation:** This chart shows which operating systems have the highest song completion rates, potentially indicating better app performance on certain platforms.

#### 3.4 Device Usage Over Time

**Chart Type:** Line Chart

**SQL Query:**
```sql
SELECT 
    dm.time_window,
    d.device_type,
    SUM(dm.stream_count) AS total_streams
FROM device_metrics dm
JOIN devices d ON dm.device_id = d.device_id
WHERE dm.time_window >= NOW() - INTERVAL '1 hour'
GROUP BY dm.time_window, d.device_type
ORDER BY dm.time_window
```

**Interpretation:** This chart shows how device usage patterns change over time, helping identify when certain devices are most active.

### 4. Geographic Dashboard

This dashboard visualizes streaming patterns across different locations.

#### 4.1 Streams by Country

**Chart Type:** World Map

**SQL Query:**
```sql
SELECT 
    l.country,
    SUM(lm.stream_count) AS total_streams
FROM location_metrics lm
JOIN locations l ON lm.location_id = l.location_id
WHERE lm.time_window >= NOW() - INTERVAL '1 hour'
GROUP BY l.country
```

**Interpretation:** This map shows which countries have the highest streaming activity, helping identify geographic hotspots.

#### 4.2 Unique Users by Region

**Chart Type:** Bar Chart

**SQL Query:**
```sql
SELECT 
    l.region,
    SUM(lm.unique_users) AS total_unique_users
FROM location_metrics lm
JOIN locations l ON lm.location_id = l.location_id
WHERE lm.time_window >= NOW() - INTERVAL '1 hour'
GROUP BY l.region
ORDER BY total_unique_users DESC
LIMIT 10
```

**Interpretation:** This chart shows which regions have the most unique users, indicating where your user base is concentrated.

#### 4.3 Streams by City Over Time

**Chart Type:** Line Chart

**SQL Query:**
```sql
SELECT 
    lm.time_window,
    l.city,
    SUM(lm.stream_count) AS total_streams
FROM location_metrics lm
JOIN locations l ON lm.location_id = l.location_id
WHERE lm.time_window >= NOW() - INTERVAL '1 hour'
AND l.city IN (
    SELECT l2.city
    FROM location_metrics lm2
    JOIN locations l2 ON lm2.location_id = l2.location_id
    WHERE lm2.time_window >= NOW() - INTERVAL '1 hour'
    GROUP BY l2.city
    ORDER BY SUM(lm2.stream_count) DESC
    LIMIT 5
)
GROUP BY lm.time_window, l.city
ORDER BY lm.time_window
```

**Interpretation:** This chart shows how streaming activity in top cities changes over time, helping identify local trends and patterns.

### 5. Hourly Trends Dashboard

This dashboard shows how key metrics change on an hourly basis.

#### 5.1 Hourly Stream Count

**Chart Type:** Line Chart

**SQL Query:**
```sql
SELECT 
    time_window,
    stream_count
FROM hourly_metrics
WHERE time_window >= NOW() - INTERVAL '24 hours'
ORDER BY time_window
```

**Interpretation:** This chart shows the total number of streams per hour, helping identify peak usage times and overall trends.

#### 5.2 Unique Users and Songs by Hour

**Chart Type:** Line Chart

**SQL Query:**
```sql
SELECT 
    time_window,
    unique_users,
    unique_songs
FROM hourly_metrics
WHERE time_window >= NOW() - INTERVAL '24 hours'
ORDER BY time_window
```

**Interpretation:** This chart compares the number of unique users and songs per hour, showing how content diversity relates to user activity.

#### 5.3 Hourly Skip and Like Rates

**Chart Type:** Line Chart

**SQL Query:**
```sql
SELECT 
    time_window,
    skip_rate,
    like_rate
FROM hourly_metrics
WHERE time_window >= NOW() - INTERVAL '24 hours'
ORDER BY time_window
```

**Interpretation:** This chart shows how skip and like rates change throughout the day, indicating when users are most satisfied with content.

#### 5.4 Average Listen Time by Hour

**Chart Type:** Bar Chart

**SQL Query:**
```sql
SELECT 
    EXTRACT(HOUR FROM time_window) AS hour_of_day,
    AVG(avg_listen_time) AS average_listen_time
FROM hourly_metrics
WHERE time_window >= NOW() - INTERVAL '24 hours'
GROUP BY EXTRACT(HOUR FROM time_window)
ORDER BY hour_of_day
```

**Interpretation:** This chart shows how average listen time varies by hour of the day, helping identify when users are most engaged.

## Real-Time Dashboard Refresh

To ensure your dashboard shows the most up-to-date data:

1. **Set Automatic Refresh:**
   - In Superset, click on the three dots in the top right corner of the dashboard
   - Select "Set auto-refresh interval"
   - Choose an appropriate interval (e.g., 30 seconds or 1 minute)

2. **Manual Refresh:**
   - Click the refresh button in the top right corner of the dashboard to manually update all charts

3. **Chart-Specific Refresh:**
   - Hover over any chart and click the refresh icon to update just that chart

## Troubleshooting

If your dashboard is not showing real-time data:

1. **Check Data Pipeline:**
   - Verify that Kafka, Spark, and PostgreSQL are running
   - Check that the Spark streaming job is processing data
   - Ensure that data is being written to the PostgreSQL tables

2. **Check Superset Connection:**
   - Verify that Superset can connect to PostgreSQL
   - Test the database connection in Superset

3. **Check SQL Queries:**
   - Verify that your SQL queries are correct
   - Test the queries directly in PostgreSQL to ensure they return data

4. **Check Time Filters:**
   - Ensure that time-based filters are using the correct time zone
   - Adjust time intervals if needed (e.g., change from 1 hour to 24 hours)

## Conclusion

This real-time dashboard provides valuable insights into your music streaming service, allowing you to monitor user engagement, content performance, device usage, and geographic patterns as they happen. By regularly reviewing these visualizations, you can identify trends, detect issues, and make data-driven decisions to improve your service.