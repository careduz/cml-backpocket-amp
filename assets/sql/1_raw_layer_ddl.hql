-- GTFS spec: https://developers.google.com/transit/gtfs/reference
-- TODO: parameterize HDFS file location
CREATE EXTERNAL TABLE IF NOT EXISTS operations.raw_stop_times ( 
    trip_id string,
    arrival_time string,
    departure_time string,
    stop_id string,
    stop_sequence int,
    stop_headsign boolean,
    pickup_type string,
    drop_off_type string,
    shape_dist_traveled float,  
    extract_time timestamp
)
COMMENT "Schedule per stop"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n"
WITH SERDEPROPERTIES (
  "timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSSZ"
)
STORED AS TEXTFILE
LOCATION "${BUCKET_NAME}/warehouse/tablespace/external/hive/operations.db/stop_times"
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE EXTERNAL TABLE IF NOT EXISTS operations.raw_trips ( 
    route_id string,
    service_id string,
    trip_id string,
    trip_headsign string,
    trip_short_name string,
    direction_id string,
    block_id string,
    shape_id string,
    wheelchair_accessible int,
    bikes_allowed int,
    extract_time timestamp
)
COMMENT "Trip details"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n"
WITH SERDEPROPERTIES (
  "timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSSZ"
)
STORED AS TEXTFILE
LOCATION "${BUCKET_NAME}/warehouse/tablespace/external/hive/operations.db/trips"
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS operations.raw_stops ( 
    stop_id string,
    stop_code string,
    stop_name string,
    stop_desc string,
    stop_lat decimal(8,6),
    stop_lon decimal(9,6),
    zone_id string,
    stop_url string,
    location_type int,
    parent_station string,
    stop_timezone string,
    wheelchair_boarding int,
    extract_time timestamp
)
COMMENT "Stop details"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n"
WITH SERDEPROPERTIES (
  "timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSSZ"
)
STORED AS TEXTFILE
LOCATION "${BUCKET_NAME}/warehouse/tablespace/external/hive/operations.db/stops"
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS operations.raw_shapes ( 
    shape_id string,
    shape_pt_lat float,
    shape_pt_lon float,
    shape_pt_sequence int,
    shape_dist_traveled float,
    extract_time timestamp
)
COMMENT "Shapes describe the path that a vehicle travels along a route alignment"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n"
WITH SERDEPROPERTIES (
  "timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSSZ"
)
STORED AS TEXTFILE
LOCATION "${BUCKET_NAME}/warehouse/tablespace/external/hive/operations.db/shapes"
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS operations.raw_routes ( 
    route_id string,
    agency_id string,
    route_short_name string,
    route_long_name string,
    route_desc string,
    route_type string,
    route_url string,
    route_color string,
    route_text_color string,
    extract_time timestamp
)
COMMENT "Route details"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n"
WITH SERDEPROPERTIES (
  "timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSSZ"
)
STORED AS TEXTFILE
LOCATION "${BUCKET_NAME}/warehouse/tablespace/external/hive/operations.db/routes"
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS operations.raw_calendar_dates ( 
    service_id string,
    `date` date,
    exception_type int,
    extract_time timestamp
)
COMMENT "Used in conjunction with calendar to define exceptions to the default service patterns defined in calendar"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n"
WITH SERDEPROPERTIES (
  "timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSSZ",
  "date.formats" = "yyyy-MM-dd"
)
STORED AS TEXTFILE
LOCATION "${BUCKET_NAME}/warehouse/tablespace/external/hive/operations.db/calendar_dates"
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS operations.raw_calendar ( 
    service_id string,
    monday string,
    tuesday string,
    wednesday string,
    thursday string,
    friday string,
    saturday string,
    sunday string,
    start_date date,
    end_date date,
    extract_time timestamp
)
COMMENT "models calendar.txt"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n"
WITH SERDEPROPERTIES (
  "timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSSZ",
  "date.formats" = "yyyy-MM-dd"
)
STORED AS TEXTFILE
LOCATION "${BUCKET_NAME}/warehouse/tablespace/external/hive/operations.db/calendar"
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS operations.raw_agency ( 
   agency_id string,
   agency_name string,
   agency_url string,
   agency_timezone string,
   agency_lang string,
   agency_phone string,
   agency_fare_url string,
   extract_time timestamp
)
COMMENT "models calendar.txt"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n"
WITH SERDEPROPERTIES (
  "timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss.SSSZ"
)
STORED AS TEXTFILE
LOCATION "${BUCKET_NAME}/warehouse/tablespace/external/hive/operations.db/agency"
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE TABLE IF NOT EXISTS operations.raw_next_vehicle_arrival (
  message_id string,
  route_id string,
  route_title string,
  stop_tag string,
  stop_title string,
  trip_tag string,
  is_departure boolean,
  is_affected_by_layover boolean,
  vehicle string,
  block string,
  dir_tag string,
  branch string,
  arriving_in_minutes int,
  arriving_in_seconds int,
  arriving_on_epoch_time timestamp,
  published_epoch_time timestamp,

  PRIMARY KEY(message_id)
)
COMMENT "Most recent Next Vehicle Arrival messages"
STORED AS KUDU;

CREATE TABLE IF NOT EXISTS operations.raw_tweets ( 
   id string,
   created_at string,
   text string,
   full_text string,
   favorited boolean,
   retweeted boolean,
   possibly_sensitive boolean,
   lang string,
   timestamp_ms timestamp,
   quote_count int,
   reply_count int,
   retweet_count int,
   favorite_count int,
   user_id bigint,
   user_name string,
   user_screen_name string,
   user_location string,
   user_url string,
   user_description string,
   user_translator_type string,
   user_protected boolean,
   user_verified boolean,
   user_followers_count int,
   user_friends_count int,
   user_listed_count int,
   user_favourites_count int,
   user_statuses_count int,
   user_created_at string,
   user_geo_enabled boolean,
   user_contributors_enabled boolean,
   user_is_translator boolean,
   user_default_profile boolean,
   hashtags string,
   retweet_of_id string,
   in_reply_to_id string,
   in_reply_to_screen_name string,
   in_reply_to_user_id string,

   PRIMARY KEY (id)
)
COMMENT "Tweets related to TTC"
STORED AS KUDU;

CREATE TABLE IF NOT EXISTS operations.raw_next_vehicle_arrival_stops ( 
    tag string,
    id string,
    longitude string,
    latitude string,
    name string,

   PRIMARY KEY (tag)
)
COMMENT "Stops mapping to join with Next Vehicle Arrivals"
STORED AS KUDU;


CREATE TABLE IF NOT EXISTS default.raw_stop_times ( 
    trip_id string,
    arrival_time string,
    departure_time string,
    stop_id string,
    stop_sequence int,
    stop_headsign boolean,
    pickup_type string,
    drop_off_type string,
    shape_dist_traveled float,  
    extract_time timestamp
)
COMMENT "Schedule per stop"
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS default.raw_trips ( 
    route_id string,
    service_id string,
    trip_id string,
    trip_headsign string,
    trip_short_name string,
    direction_id string,
    block_id string,
    shape_id string,
    wheelchair_accessible int,
    bikes_allowed int,
    extract_time timestamp
)
COMMENT "Trip details"
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS default.raw_stops ( 
    stop_id string,
    stop_code string,
    stop_name string,
    stop_desc string,
    stop_lat decimal(8,6),
    stop_lon decimal(9,6),
    zone_id string,
    stop_url string,
    location_type int,
    parent_station string,
    stop_timezone string,
    wheelchair_boarding int,
    extract_time timestamp
)
COMMENT "Stop details"
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS default.raw_shapes ( 
    shape_id string,
    shape_pt_lat float,
    shape_pt_lon float,
    shape_pt_sequence int,
    shape_dist_traveled float,
    extract_time timestamp
)
COMMENT "Shapes describe the path that a vehicle travels along a route alignment"
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS default.raw_routes ( 
    route_id string,
    agency_id string,
    route_short_name string,
    route_long_name string,
    route_desc string,
    route_type string,
    route_url string,
    route_color string,
    route_text_color string,
    extract_time timestamp
)
COMMENT "Route details"
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS default.raw_calendar_dates ( 
    service_id string,
    `date` date,
    exception_type int,
    extract_time timestamp
)
COMMENT "Used in conjunction with calendar to define exceptions to the default service patterns defined in calendar"
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS default.raw_calendar ( 
    service_id string,
    monday string,
    tuesday string,
    wednesday string,
    thursday string,
    friday string,
    saturday string,
    sunday string,
    start_date date,
    end_date date,
    extract_time timestamp
)
COMMENT "models calendar.txt"
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS default.raw_agency ( 
   agency_id string,
   agency_name string,
   agency_url string,
   agency_timezone string,
   agency_lang string,
   agency_phone string,
   agency_fare_url string,
   extract_time timestamp
)
COMMENT "models calendar.txt"
STORED AS PARQUET;