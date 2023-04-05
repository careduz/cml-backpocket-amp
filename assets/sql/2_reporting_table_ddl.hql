create view operations.reporting_tweets as (
    select
        user_screen_name, concat("@", user_screen_name ) as user_twitter_handle,
        coalesce(full_text, text) as content,
        concat("https://www.twitter.com/", user_screen_name,"/status/",id) as tweet_url,
        case 
          when user_screen_name = "TTCnotices" or user_screen_name = "TTChelps" or user_screen_name = "TTCNewsroom" or user_screen_name = "TTCplanning"
          then true 
          else false
        end
        as from_ttc, 
        case
          when user_screen_name = "TTCnotices" then true
          else false
        end
        as is_notice,
        case
            when retweet_of_id is null then false
            else true
        end as is_retweet,
        case
            when in_reply_to_id is null then false
            else true
        end as is_reply,
        case
          when in_reply_to_id is not null then "reply"
          when retweet_of_id is not null then "retweet"
          else "first"
        end as tweet_type,
        user_name,
        retweet_of_id,
        in_reply_to_id,
        in_reply_to_screen_name,
        in_reply_to_user_id
        quote_count,
        reply_count,
        retweet_count,
        favorite_count,
        user_url,
        user_location,
        user_followers_count,
        user_favourites_count,
        user_verified,
        user_statuses_count,
        user_created_at,
        hashtags,
        user_protected,
        possibly_sensitive,
        id,
        timestamp_ms
    from operations.raw_tweets
);

create view operations.reporting_twitter_handles as (
  select
        user_screen_name,
        user_twitter_handle,
        user_name,
        user_url,
        user_location,
        user_followers_count,
        user_favourites_count,
        user_location_clean,
        user_verified,
        user_statuses_count,
        user_created_at,
        hashtags,
        user_protected,
        possibly_sensitive,
        last_tweet,
        id,
        timestamp_ms 
    from (
    select
    row_number() over(partition by user_id order by timestamp_ms desc) as rownumber,
        user_screen_name,
        concat("@", user_screen_name ) as user_twitter_handle,
        concat("https://www.twitter.com/", user_screen_name) as handle_url,
        user_name,
        user_url,
        user_location,
        case
          when lower(user_location) like "%toronto%" then "Toronto, Ontario"
          else user_location
        end as user_location_clean,
        user_followers_count,
        coalesce(full_text, text) as last_tweet,
        user_favourites_count,
        user_verified,
        user_statuses_count,
        user_created_at,
        hashtags,
        user_protected,
        possibly_sensitive,
        id,
        from_utc_timestamp(timestamp_ms, 'EDT') as timestamp_ms
    from operations.raw_tweets
    where possibly_sensitive is null or possibly_sensitive is false
    and user_screen_name not in ("TTCnotices", "TTChelps", "TTCNewsroom", "TTCplanning")
  ) as ranked 
    where ranked.rownumber = 1
);

create view operations.reporting_next_scheduled_arrivals as (
  select * from (
    select 
      routes.route_short_name,
      routes.route_long_name,
      direction_id,
      trip_headsign,
      stops.stop_code,
      stops.stop_name,
      stop_times.stop_sequence,
      to_utc_timestamp(concat(cast(to_date(from_utc_timestamp(now(), "US/Eastern")) as string)," ", departure_time), 'US/Eastern') as scheduled_at_utc_time,
      stops.stop_lon as longitude,
      stops.stop_lat as latitude,
       case 
        when routes.route_type = "0" then "streetcar"
        when routes.route_type = "1" then "subway"
        when routes.route_type = "3" then "bus"
      end as route_type,
      row_number() over(partition by routes.route_short_name, direction_id, stops.stop_code order by unix_timestamp(to_utc_timestamp(concat(cast(to_date(from_utc_timestamp(now(), "US/Eastern")) as string)," ", departure_time), 'US/Eastern')) asc) as rownumber
    from operations.raw_trips trips
    inner join operations.raw_stop_times stop_times on stop_times.trip_id = trips.trip_id
    inner join operations.raw_stops stops on stops.stop_id = stop_times.stop_id
    inner join operations.raw_routes routes on trips.route_id = routes.route_id
    where 1 = 1
      and 
      case 
        when dayofweek(from_utc_timestamp(now(), "US/Eastern")) = 7 then trips.service_id = "2"
        when dayofweek(from_utc_timestamp(now(), "US/Eastern")) = 1 then trips.service_id = "3"
        else trips.service_id = "1"
      end
      and to_utc_timestamp(concat(cast(to_date(from_utc_timestamp(now(), "US/Eastern")) as string)," ", departure_time), 'US/Eastern') >= now()
    group by 1,2,3,4,5,6,7,8,9,10,11
    ) as all_stops
    where rownumber = 1
    order by scheduled_at_utc_time desc
);

create view operations.reporting_next_predicted_arrivals as (
  select s.id as stop_id, * from (
  select 
    route_id as route_short_name,
    route_title as route_long_name,
    stop_tag,
    stop_title as stop_name,
    trip_tag,
    vehicle as vehicle_number,
    arriving_on_epoch_time,
    published_epoch_time,
    arriving_in_minutes,
    arriving_in_seconds,
    row_number() over(partition by route_id, stop_tag order by arriving_on_epoch_time) as rownumber
  from operations.raw_next_vehicle_arrival
  where 
    arriving_on_epoch_time >= now()
    and route_id = branch
  ) as a
  inner join operations.raw_next_vehicle_arrival_stops s on a.stop_tag = s.tag
  where rownumber=1
  and s.id is not null
  order by published_epoch_time desc
);

create view operations.reporting_current_arrival_delay as (
  select 
    *
    , case 
      when delay_minutes < -5 then "early"
      when delay_minutes >= 5 then "delayed"
      else "on-time"
    end as delay_category
  from (
    select 
      pred.route_short_name,
      pred.stop_tag,
      sched.stop_code,
      direction_id,
      pred.stop_name,
      trip_headsign,
      from_utc_timestamp(scheduled_at_utc_time, "US/Eastern") as scheduled_at_local_time,
      from_utc_timestamp(arriving_on_epoch_time, "US/Eastern") as predicted_at_local_time,
      stop_sequence,
      round((unix_timestamp(scheduled_at_utc_time) - unix_timestamp(arriving_on_epoch_time))/60) as delay_minutes,
      round((unix_timestamp(arriving_on_epoch_time) - unix_timestamp(now()))/60) as arriving_in_minutes,
      sched.latitude,
      sched.longitude,
      from_utc_timestamp(pred.published_epoch_time, "US/Eastern") as published_local_time
    from operations.reporting_next_predicted_arrivals pred
    inner join operations.reporting_next_scheduled_arrivals sched
    on 
      pred.route_short_name = sched.route_short_name 
      and pred.stop_id = sched.stop_code
    ) a
    order by route_short_name, direction_id, stop_sequence
);

create view operations.reporting_route_stop_profile as (
  select 
    routes.route_short_name,
    routes.route_long_name,
    stops.stop_code,
    stops.stop_name,
    case 
      when stops.wheelchair_boarding = 1 then true
      when stops.wheelchair_boarding = 2 then false
    end as stop_wheelchair_boarding,
    stops.stop_lon as longitude,
    stops.stop_lat as latitude
  from operations.raw_trips trips
  inner join operations.raw_stop_times stop_times on stop_times.trip_id = trips.trip_id
  inner join operations.raw_stops stops on stops.stop_id = stop_times.stop_id
  inner join operations.raw_routes routes on trips.route_id = routes.route_id
  group by 1,2,3,4,5,6,7
);

create view operations.reporting_route_trip_profile as (
  select 
  distinct 
    routes.route_short_name,
    routes.route_long_name,
    trips.trip_id,
    trips.trip_headsign,
    case 
      when trips.wheelchair_accessible = 1 then true
      when trips.wheelchair_accessible = 2 then false
    end as trip_wheelchair_accessible,
    case 
      when trips.bikes_allowed = 1 then true
      when trips.bikes_allowed = 2 then false
    end as trip_bikes_allowed
  from operations.raw_trips trips
  inner join operations.raw_stop_times stop_times on stop_times.trip_id = trips.trip_id
  inner join operations.raw_stops stops on stops.stop_id = stop_times.stop_id
  inner join operations.raw_routes routes on trips.route_id = routes.route_id
);
