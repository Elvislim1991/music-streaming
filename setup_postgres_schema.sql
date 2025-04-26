-- Create dimension tables

-- Users dimension
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    age INT,
    gender VARCHAR(50),
    subscription_type VARCHAR(50),
    registration_date DATE,
    country VARCHAR(100)
);

-- Artists dimension
CREATE TABLE IF NOT EXISTS artists (
    artist_id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    country VARCHAR(100),
    genre VARCHAR(100),
    is_band BOOLEAN
);

-- Albums dimension
CREATE TABLE IF NOT EXISTS albums (
    album_id INT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    artist_id INT REFERENCES artists(artist_id),
    release_date DATE,
    total_tracks INT,
    album_type VARCHAR(50)
);

-- Songs dimension
CREATE TABLE IF NOT EXISTS songs (
    song_id INT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    duration_seconds INT,
    release_date DATE,
    genre VARCHAR(100),
    language VARCHAR(50),
    explicit BOOLEAN
);

-- Devices dimension
CREATE TABLE IF NOT EXISTS devices (
    device_id INT PRIMARY KEY,
    device_type VARCHAR(100),
    os VARCHAR(100),
    browser VARCHAR(100),
    app_version VARCHAR(50)
);

-- Locations dimension
CREATE TABLE IF NOT EXISTS locations (
    location_id INT PRIMARY KEY,
    country VARCHAR(100),
    region VARCHAR(100),
    city VARCHAR(100),
    timezone VARCHAR(100)
);

-- Create fact table
CREATE TABLE IF NOT EXISTS stream_events (
    event_id UUID PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    song_id INT REFERENCES songs(song_id),
    artist_id INT REFERENCES artists(artist_id),
    album_id INT REFERENCES albums(album_id),
    device_id INT REFERENCES devices(device_id),
    location_id INT REFERENCES locations(location_id),
    timestamp TIMESTAMP NOT NULL,
    stream_duration_seconds INT,
    is_complete_play BOOLEAN,
    is_skipped BOOLEAN,
    is_liked BOOLEAN,
    is_shared BOOLEAN,
    is_added_to_playlist BOOLEAN
);

-- Create analytics tables for pre-aggregated metrics

-- User engagement metrics
CREATE TABLE IF NOT EXISTS user_engagement_metrics (
    time_window TIMESTAMP,
    user_id INT REFERENCES users(user_id),
    stream_count BIGINT,
    total_listen_time BIGINT,
    avg_listen_time DOUBLE PRECISION,
    skip_rate DOUBLE PRECISION,
    like_rate DOUBLE PRECISION,
    share_rate DOUBLE PRECISION,
    playlist_add_rate DOUBLE PRECISION,
    PRIMARY KEY (time_window, user_id)
);

-- Content performance metrics
CREATE TABLE IF NOT EXISTS content_performance_metrics (
    time_window TIMESTAMP,
    song_id INT REFERENCES songs(song_id),
    artist_id INT REFERENCES artists(artist_id),
    stream_count BIGINT,
    completion_rate DOUBLE PRECISION,
    skip_rate DOUBLE PRECISION,
    like_rate DOUBLE PRECISION,
    PRIMARY KEY (time_window, song_id, artist_id)
);

-- Device metrics
CREATE TABLE IF NOT EXISTS device_metrics (
    time_window TIMESTAMP,
    device_id INT REFERENCES devices(device_id),
    stream_count BIGINT,
    avg_listen_time DOUBLE PRECISION,
    completion_rate DOUBLE PRECISION,
    PRIMARY KEY (time_window, device_id)
);

-- Location metrics
CREATE TABLE IF NOT EXISTS location_metrics (
    time_window TIMESTAMP,
    location_id INT REFERENCES locations(location_id),
    stream_count BIGINT,
    unique_users BIGINT,
    unique_songs BIGINT,
    PRIMARY KEY (time_window, location_id)
);

-- Hourly metrics
CREATE TABLE IF NOT EXISTS hourly_metrics (
    time_window TIMESTAMP PRIMARY KEY,
    stream_count BIGINT,
    unique_users BIGINT,
    unique_songs BIGINT,
    unique_artists BIGINT,
    total_listen_time BIGINT,
    avg_listen_time DOUBLE PRECISION,
    skip_rate DOUBLE PRECISION,
    like_rate DOUBLE PRECISION
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_stream_events_timestamp ON stream_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_stream_events_user_id ON stream_events(user_id);
CREATE INDEX IF NOT EXISTS idx_stream_events_song_id ON stream_events(song_id);
CREATE INDEX IF NOT EXISTS idx_stream_events_artist_id ON stream_events(artist_id);
CREATE INDEX IF NOT EXISTS idx_stream_events_device_id ON stream_events(device_id);
CREATE INDEX IF NOT EXISTS idx_stream_events_location_id ON stream_events(location_id);

-- Create indexes for metrics tables
CREATE INDEX IF NOT EXISTS idx_user_engagement_time_window ON user_engagement_metrics(time_window);
CREATE INDEX IF NOT EXISTS idx_content_performance_time_window ON content_performance_metrics(time_window);
CREATE INDEX IF NOT EXISTS idx_device_metrics_time_window ON device_metrics(time_window);
CREATE INDEX IF NOT EXISTS idx_location_metrics_time_window ON location_metrics(time_window);