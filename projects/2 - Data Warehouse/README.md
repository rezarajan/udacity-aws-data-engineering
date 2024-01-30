### Entity Relationship Diagram (ERD)
Below you can find the ERD for the database, containing the staging tables and the fact and dimension tables.

![erd](images/erd.png 'ERD')

### Sample Queries

```sql
-- Table entry counts
SELECT 'staging_events' AS table_name, COUNT(*) AS row_count FROM staging_events
UNION SELECT 'staging_songs' AS table_name, COUNT(*) AS row_count FROM staging_songs
UNION SELECT 'f_songplay' AS table_name, COUNT(*) AS row_count FROM f_songplay
UNION SELECT 'd_artist' AS table_name, COUNT(*) AS row_count FROM d_artist
UNION SELECT 'd_song' AS table_name, COUNT(*) AS row_count FROM d_song
UNION SELECT 'd_user' AS table_name, COUNT(*) AS row_count FROM d_user
UNION SELECT 'd_time' AS table_name, COUNT(*) AS row_count FROM d_time
ORDER BY row_count DESC;
```

|table_name|row_count|
|----------|---------|
|staging_songs|385252|
|d_song|384995|
|d_artist|45266|
|staging_events|8056|
|f_songplay|6964|
|d_time|6487|
|d_user|105|


```sql
-- Top 10 most played artists for the year by play time
SELECT
    a.name AS artist_name,
    SUM(s.duration) AS play_time
FROM f_songplay f
JOIN d_song s ON s.song_id = f.song_id
JOIN d_artist a ON a.artist_id = f.artist_id
JOIN d_time t on t.start_time = f.start_time
WHERE t.year = 2018
GROUP BY a.name
ORDER BY 2 desc
LIMIT 10;
```

|artist_name|play_time|
|-----------|---------|
|Three Drives|49525.76240000002|
|Kanye West|31538.1565|
|3 Drives On A Vinyl|24762.881200000025|
|Coldplay|19392.333560000003|
|O'Rosko Raricim|18850.842619999996|
|The Killers / Wild Light / Mariachi El Bronx|17331.511530000007|
|Killers|17331.511530000007|
|The Killers / Toni Halliday|17331.511530000007|
|The Killers / Lou Reed|17331.511530000007|
|The Killers|17331.511530000007|


```sql
-- Top 10 most played artists for the year by play count
SELECT
    a.name AS artist_name,
    COUNT(f.songplay_id) AS total_plays
FROM f_songplay f
JOIN d_artist a ON a.artist_id = f.artist_id
JOIN d_time t on t.start_time = f.start_time
WHERE t.year = 2018
GROUP BY a.name
ORDER BY 2 DESC, 1 ASC
LIMIT 10;
```

|artist_name|total_plays|
|-----------|-----------|
|Three Drives|110|
|Kanye West|82|
|Amy Winehouse|75|
|Coldplay|58|
|Killers|57|
|The Killers|57|
|The Killers / Lou Reed|57|
|The Killers / Toni Halliday|57|
|The Killers / Wild Light / Mariachi El Bronx|57|
|Kings Of Leon|56|