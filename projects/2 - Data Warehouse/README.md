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
|f_songplay|7272|
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
|Three Drives|49525.76240000001|
|Kanye West|48078.28928000006|
|Franz Ferdinand|29370.03556000002|
|O'Rosko Raricim|28111.96726|
|Coldplay|26307.352010000017|
|Nate Dogg / Eminem / Obie Trice / Bobby Creekwater|25856.8721|
|Eminem / Dr. Dre|25451.164790000003|
|Eminem / Bizarre|25228.88933|
|Eminem / Paul "Bunyan" Rosenburg|25228.88933|
|Eminem / Dr. Dre / 50 Cent|25228.88933|

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
|Kanye West|112|
|Three Drives|110|
|Kings Of Leon|77|
|Amy Winehouse|75|
|Coldplay|71|
|O'Rosko Raricim|69|
|Usher|66|
|Killers|62|
|The Killers|62|
|The Killers / Lou Reed|62|


We can see that from this simple query, while Franz Ferdinand is a top 10 in terms of play time, it is not the case for the number of song plays itself. Redshift allows us to quickly query all this data expeditiously.
