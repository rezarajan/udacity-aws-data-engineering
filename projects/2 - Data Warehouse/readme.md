# Project Overview
In this project, we are tasked with building a data warehouse in the cloud using AWS Redshift. The intent of the warehouse is to be used for analytics purposes, with the data context being song streaming data. Furthermore, this data is currently stored in S3 as JSON files, and we must build a pipeline which appropriately ingests the data.

## Quick Start
For the full solution, please see the [Jupyter notebook](p2_cloud_data_warehouse.ipynb). This notebook contains the test run of all the code, with time checks, and reasoning where necessary.

### Scripts
All scripts are provided in the `scripts` directory, and contain docstrings for clarity:
|Script|Comment|
|---|---|
|helpers.py|Stores commonly used functions|
|sql_queries.py|Contains all queries used to create tables, and perform ELT operations|
|create_tables.py|Creates required table structures|
|etl.py|Coordinates load and transform operations|
|create_resources.py|Creates all required AWS resources for the project|
|delete_resources.py|Deletes all spun-up AWS resources for the project|

### Config File
All resource definitions should be contained in a file called `dwh.cfg` in the project's root directory. A sample file called `dwh.cfg.sample` is provided as a template. Git is configured to ignore the config file, if created, for security purposes. 

### Running Locally
If running the code locally, the steps below may be performed.

**Create Cluster and ELT**
```sh
cp dwh.cfg.sample dwh.cfg; # edit the dwh.cfg file accordingly
python scripts/delete_resources.py; # delete all existing resources (if any)
python scripts/create_resources.py # create all resources
python scripts/create_tables.py # create the table structure
python scripts/etl.py load # load data from S3 to the staging tables
python scripts/etl.py etl # transform data into the dimensional model
```

Please note that the etl scripts take quite some time to run (~1 hour each).

**Deleting All Resources**
```sh
python scripts/delete_resources.py; # delete all resources once done
```

## Data Overview
Specifically, we are provided with two categories of data:
1. User activity logs: includes song play data
2. Song metadata: includes data about the song, such as name, duration and artist.

However, given that these files are stored as JSON files, we are also provided with a log json path file, which provides the data structure. Fortunately, we may employ the use of the `COPY` command in Redshift, which allows the data to be ingested in parallel.

## Solution Overview
The general solution of such a task includes the following high-level steps:
1. Create staging tables where the raw data will land.
2. Create the dimensional model which the data will be transformed into.
3. Extract and load the data into the staging tables.
4. Transform data from the staging tables into the fact and dimension tables.


### Entity Relationship Diagram (ERD)
Below you can find the ERD for the database, containing the staging tables and the fact and dimension tables.

![erd](images/erd.png 'ERD')

| Table Name | Table Type | Comment |
|---|---|---|
|staging_events|Staging|Stores user activity logs|
|staging_songs|Staging|Stores song metadata|
|f_songplay|Fact|User activity data for song plays only|
|d_time|Dimension|Standard time table for join operations|
|d_user|Dimension|User data generated from the user activity logs|
|d_song|Dimension|Song data extracted from the song metadata|
|d_artist|Dimension|Artist data extracted from the song metadata|


### Optimization
It's critical to consider the types of queries that will be run, so as to improve analytical query performance. Some sample queries are outlined in the next section. Using these queries as a framework, we employ the use of DISTKEYs and SORTKEYs in the fact and dimension tables to optimize join operations and reduce shuffling. Thus, the following assertions are made:
1. d_time is distributed across all slices (ALL), since the data contained will be relatively small.
2. All other tables are left to the default (AUTO) diststyle.
3. All primary keys are used as a sortkey, except in f_songplay.
4. In f_songplay, start_time is used as the sortkey as it makes sense that song play data should be chronologically sorted.

### Sample Queries

```sql
-- Table row counts
select 'staging_events' as table_name, count(*) as row_count from staging_events
union select 'staging_songs' as table_name, count(*) as row_count from staging_songs
union select 'f_songplay' as table_name, count(*) as row_count from f_songplay
union select 'd_artist' as table_name, count(*) as row_count from d_artist
union select 'd_song' as table_name, count(*) as row_count from d_song
union select 'd_user' as table_name, count(*) as row_count from d_user
union select 'd_time' as table_name, count(*) as row_count from d_time
order by 2 desc;
```


```sql
-- Top played artists
select 
	da.name,
	count(fs.song_id) as song_play_count
from f_songplay fs
join (
	select
		artist_id,
		name,
		row_number() over (partition by artist_id order by name) as row_number
	from d_artist
) da on da.artist_id = fs.artist_id and da.row_number = 1
group by da.name
order by 2 desc
limit 10;
```

```sql
-- Top played songs by listen time
select
	ds.title as song_title,
	da.name as artist_name,
	sum(ds.duration) as song_play_time
from f_songplay fs
join d_song ds on ds.song_id = fs.song_id
join (
	select
		artist_id,
		name,
		row_number() over (partition by artist_id order by name) as row_number
	from d_artist
) da on da.artist_id = fs.artist_id and da.row_number = 1
group by 1,2
order by 3 desc
limit 10;
```

## Suggestions
One note on the data is that there is a one-to-many relationship between `artist_id` and `name` in the `d_artist` table. Upon further exploration, this is a consequence of the source data, wherein an artist may either have a solo song, or have backing/guest artists (e.g. artist_id 1234 may correspond to "Artist A", and also "Artist A; Artist B"). Since this is a table commonly join on, it is suggested to either:
1. Perform cleaning on the source data or
2. Enrich the source data with the row_number() logic as shown in the sample queries.

Either way, this will reduce the overhead of - and enhance the readability of - the analytical queries.