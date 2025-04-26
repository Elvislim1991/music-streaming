# Music Streaming Analytics - Star Schema Design

## Overview
This document outlines the star schema design for the music streaming analytics pipeline. The schema is optimized for analyzing streaming events and computing various metrics.

## Fact Table: StreamEvents
The central fact table that records each streaming event.

| Column | Type | Description |
|--------|------|-------------|
| event_id | UUID | Unique identifier for each streaming event |
| user_id | INT | Foreign key to Users dimension |
| song_id | INT | Foreign key to Songs dimension |
| artist_id | INT | Foreign key to Artists dimension |
| album_id | INT | Foreign key to Albums dimension |
| device_id | INT | Foreign key to Devices dimension |
| location_id | INT | Foreign key to Locations dimension |
| timestamp | TIMESTAMP | When the streaming event occurred |
| stream_duration_seconds | INT | How long the user streamed the song |
| is_complete_play | BOOLEAN | Whether the song was played completely |
| is_skipped | BOOLEAN | Whether the song was skipped |
| is_liked | BOOLEAN | Whether the user liked the song |
| is_shared | BOOLEAN | Whether the user shared the song |
| is_added_to_playlist | BOOLEAN | Whether the song was added to a playlist |

## Dimension Tables

### Users
Information about the users of the streaming service.

| Column | Type | Description |
|--------|------|-------------|
| user_id | INT | Primary key |
| username | VARCHAR | User's username |
| age | INT | User's age |
| gender | VARCHAR | User's gender |
| subscription_type | VARCHAR | Type of subscription (free, premium, etc.) |
| registration_date | DATE | When the user registered |
| country | VARCHAR | User's country |

### Songs
Information about the songs available on the streaming service.

| Column | Type | Description |
|--------|------|-------------|
| song_id | INT | Primary key |
| title | VARCHAR | Song title |
| duration_seconds | INT | Song duration in seconds |
| release_date | DATE | When the song was released |
| genre | VARCHAR | Song genre |
| language | VARCHAR | Song language |
| explicit | BOOLEAN | Whether the song has explicit content |

### Artists
Information about the artists.

| Column | Type | Description |
|--------|------|-------------|
| artist_id | INT | Primary key |
| name | VARCHAR | Artist name |
| country | VARCHAR | Artist's country of origin |
| genre | VARCHAR | Primary genre of the artist |
| is_band | BOOLEAN | Whether the artist is a band or solo artist |

### Albums
Information about music albums.

| Column | Type | Description |
|--------|------|-------------|
| album_id | INT | Primary key |
| title | VARCHAR | Album title |
| artist_id | INT | Foreign key to Artists dimension |
| release_date | DATE | When the album was released |
| total_tracks | INT | Number of tracks in the album |
| album_type | VARCHAR | Type of album (LP, EP, Single, etc.) |

### Devices
Information about the devices used for streaming.

| Column | Type | Description |
|--------|------|-------------|
| device_id | INT | Primary key |
| device_type | VARCHAR | Type of device (mobile, desktop, smart speaker, etc.) |
| os | VARCHAR | Operating system |
| browser | VARCHAR | Browser (if applicable) |
| app_version | VARCHAR | Version of the streaming app |

### Locations
Information about the locations where streaming occurs.

| Column | Type | Description |
|--------|------|-------------|
| location_id | INT | Primary key |
| country | VARCHAR | Country |
| region | VARCHAR | Region/State/Province |
| city | VARCHAR | City |
| timezone | VARCHAR | Timezone |

## Metrics to Compute

Using this star schema, we can compute various metrics including:

1. **User Engagement Metrics**:
   - Total listening time per user
   - Average session duration
   - Number of songs streamed
   - Skip rate
   - Like rate

2. **Content Performance Metrics**:
   - Most streamed songs
   - Most streamed artists
   - Most streamed genres
   - Skip rate by song/artist/genre

3. **Temporal Metrics**:
   - Streaming patterns by time of day
   - Streaming patterns by day of week
   - Trending songs over time

4. **Geographic Metrics**:
   - Streaming patterns by location
   - Popular genres by region

5. **Device Metrics**:
   - Streaming patterns by device type
   - User experience metrics by device/OS

This star schema design provides a flexible foundation for analyzing streaming behavior and generating valuable insights for the music streaming service.