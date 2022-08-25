
-- Create the database
CREATE OR REPLACE DATABASE YOUR_DATABASE;

-- Create the schema
CREATE OR REPLACE SCHEMA YOUR_DATABASE.MEETUP;

-- Create the file format for the csv files
CREATE OR REPLACE FILE FORMAT YOUR_DATABASE.MEETUP.FORMAT_TEST 
TYPE=CSV
COMPRESSION = 'AUTO' FIELD_DELIMITER = ',' RECORD_DELIMITER = '\n' 
SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' TRIM_SPACE = FALSE 
ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE ESCAPE = 'NONE' ESCAPE_UNENCLOSED_FIELD = '\134' 
DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('\\N');

-- Create a stage. Make sure you have the files downloaded and change the path.
CREATE STAGE csvfiles;

PUT file:///C:\Users\Honey\Downloads\archive\*.csv @csvfiles;

-- Creating the tables

-- Categories;
CREATE OR REPLACE TABLE YOUR_DATABASE.MEETUP.CATEGORIES (
category_id       int,
category_name    text,
shortname        text,
sort_name        text
);

COPY INTO YOUR_DATABASE.MEETUP.CATEGORIES(category_id, category_name, shortname, sort_name)
FROM (
  SELECT t.$1,t.$2,t.$3, t.$4
  FROM @csvfiles/categories.csv.gz t
)
file_format = (format_name = YOUR_DATABASE.MEETUP.FORMAT_TEST);

-- Cities;
CREATE OR REPLACE TABLE YOUR_DATABASE.MEETUP.CITIES(
city                       text,
city_id                    int,
country                    varchar(2),
distance                   numeric,
latitude                   numeric,
localized_country_name     varchar(3),
longitude                  numeric,
member_count               bigint,
ranking                    int,
state                      varchar(2),
zip                        int
);

COPY INTO YOUR_DATABASE.MEETUP.CITIES(city, city_id, country, distance, latitude,localized_country_name, longitude, member_count,ranking, state, zip)
FROM (
  SELECT t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,t.$9,t.$10,t.$11
  FROM @csvfiles/cities.csv.gz t
)
file_format = (format_name = YOUR_DATABASE.MEETUP.FORMAT_TEST);

-- Events
CREATE OR REPLACE TABLE YOUR_DATABASE.MEETUP.EVENTS
(
	event_id                         VARCHAR(12),
	created                          datetime,
	description                      text,
	duration                         int,
	event_url                        text,
	"fee.accepts"                    text,
	"fee.amount"                     numeric,
	"fee.currency"                   text,
	"fee.description"                text,
	"fee.label"                      text,
	"fee.required"                   smallint,
	"group.created"                  datetime,
	"group.group_lat"                numeric,
	"group.group_lon"                numeric,
	"group_id"                       bigint,
	"group.join_mode"                varchar(100),
	"group.name"                     text,
	"group.urlname"                  text,
	"group.who"                      varchar(255),
	headcount                        smallint,
	how_to_find_us                   text,
	maybe_rsvp_count                 smallint,
	event_name                       text,
	photo_url                        varchar(255),
	"rating.average"                 numeric,
	"rating.count"                   int,
	rsvp_limit                       int,
	event_status                     varchar(255),
	event_time                       datetime,
	updated                          datetime,
	utc_offset                       int,
	"venue.address_1"                text,
	"venue.address_2"                text,
	"venue.city"                     varchar(255),
	"venue.country"                  varchar(2),
	venue_id                         bigint,
	"venue.lat"                      numeric,
	"venue.localized_country_name"   varchar(3),
	"venue.lon"                      numeric,
	"venue.name"                     text,
	"venue.phone"                    bigint,
	"venue.repinned"                 smallint,
	"venue.state"                    varchar(100),
	"venue.zip"                      smallint,
	visibility                       varchar(100),
	waitlist_count                   int,
	why                              varchar(25),
	yes_rsvp_count                   int
);

COPY INTO YOUR_DATABASE.MEETUP.EVENTS(event_id, created, description, duration, event_url,
       "fee.accepts", "fee.amount", "fee.currency", "fee.description",
       "fee.label", "fee.required", "group.created", "group.group_lat",
       "group.group_lon", "group_id", "group.join_mode", "group.name",
       "group.urlname", "group.who", headcount, how_to_find_us,
       maybe_rsvp_count, event_name, photo_url, "rating.average",
       "rating.count", rsvp_limit, event_status, event_time, updated,
       utc_offset, "venue.address_1", "venue.address_2", "venue.city",
       "venue.country", venue_id, "venue.lat",
       "venue.localized_country_name", "venue.lon", "venue.name",
       "venue.phone", "venue.repinned", "venue.state", "venue.zip",
       visibility, waitlist_count, why, yes_rsvp_count)
FROM (
  SELECT t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,t.$9,t.$10,
  t.$11,t.$12,t.$13,t.$14,t.$15,t.$16,t.$17,t.$18,t.$19,t.$20,
  t.$21,t.$22,t.$23,t.$24,t.$25,t.$26,t.$27,t.$28,t.$29,t.$30,
  t.$31,t.$32,t.$33,t.$34,t.$35,t.$36,t.$37,t.$38,t.$39,t.$40,
  t.$41,t.$42,t.$43,t.$44,t.$45,t.$46,t.$47,t.$48
  FROM @csvfiles/events.csv.gz t
)
file_format = (format_name = YOUR_DATABASE.MEETUP.FORMAT_TEST);

-- Groups
CREATE OR REPLACE TABLE YOUR_DATABASE.MEETUP.GROUPS
(
	group_id                          bigint,
	category_id                       int,
	"category.name"                   text,
	"category.shortname"              text,
	city_id                           int,
	city                              varchar(255),
	country                           varchar(2),
	created                           datetime,
	description                       text,
	"group_photo.base_url"            text,
	"group_photo.highres_link"        text,
	"group_photo.photo_id"            bigint,
	"group_photo.photo_link"          text,
	"group_photo.thumb_link"          text,
	"group_photo.type"                varchar(100),
	join_mode                         varchar(25),
	lat                               numeric,
	link                              text,
	lon                               numeric,
	members                           int,
	group_name                        text,
	"organizer.member_id"             bigint,
	"organizer.name"                  varchar(255),
	"organizer.photo.base_url"        text,
	"organizer.photo.highres_link"    text,
	"organizer.photo.photo_id"        bigint,
	"organizer.photo.photo_link"      text,
	"organizer.photo.thumb_link"      text,
	"organizer.photo.type"            varchar(100),
	rating                            numeric,
	state                             varchar(2),
	timezone                          varchar(25),
	urlname                           text,
	utc_offset                        numeric,
	visibility                        varchar(25),
	who                               text
);

COPY INTO YOUR_DATABASE.MEETUP.GROUPS(group_id,category_id,"category.name","category.shortname",city_id,
city,country,created,description,"group_photo.base_url","group_photo.highres_link","group_photo.photo_id",
"group_photo.photo_link","group_photo.thumb_link","group_photo.type",join_mode,lat,link,lon,members,
group_name,"organizer.member_id","organizer.name","organizer.photo.base_url",
"organizer.photo.highres_link","organizer.photo.photo_id","organizer.photo.photo_link",
"organizer.photo.thumb_link","organizer.photo.type",rating,state,timezone,urlname,utc_offset,visibility,who)
FROM (
  SELECT t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,t.$9,t.$10,
  t.$11,t.$12,t.$13,t.$14,t.$15,t.$16,t.$17,t.$18,t.$19,t.$20,
  t.$21,t.$22,t.$23,t.$24,t.$25,t.$26,t.$27,t.$28,t.$29,t.$30,
  t.$31,t.$32,t.$33,t.$34,t.$35,t.$36
  FROM @csvfiles/groups.csv.gz t
)
file_format = (format_name = YOUR_DATABASE.MEETUP.FORMAT_TEST);


-- Groups Topics;
CREATE OR REPLACE TABLE YOUR_DATABASE.MEETUP.GROUPS_TOPICS(
topic_id       bigint,
topic_key     text,
topic_name    text,
group_id       bigint
);

COPY INTO YOUR_DATABASE.MEETUP.GROUPS_TOPICS(topic_id, topic_key, topic_name, group_id)
FROM (
  SELECT t.$1,t.$2,t.$3,t.$4
  FROM @csvfiles/groups_topics.csv.gz t
)
file_format = (format_name = YOUR_DATABASE.MEETUP.FORMAT_TEST, encoding= 'ISO88591');


-- Members;
CREATE OR REPLACE TABLE YOUR_DATABASE.MEETUP.MEMBERS(
member_id          int,
bio                text,
city               varchar(100),
country            varchar(2),
hometown           varchar(100),
joined             datetime,
lat                numeric,
link               text,
lon                numeric,
member_name        varchar(255),
state              varchar(10),
member_status      varchar(25),
visited            date,
group_id           bigint
);

COPY INTO YOUR_DATABASE.MEETUP.MEMBERS(member_id, bio, city, country, hometown, joined, lat,
       link, lon, member_name, state, member_status, visited,
       group_id)
FROM (
 SELECT t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,t.$9,t.$10,
  t.$11,t.$12,t.$13,t.$14
  FROM @csvfiles/members.csv.gz t
)
file_format = (format_name = YOUR_DATABASE.MEETUP.FORMAT_TEST, encoding= 'ISO88591');


-- Members Topics;
CREATE OR REPLACE TABLE YOUR_DATABASE.MEETUP.MEMBERS_TOPICS(
topic_id       bigint,
topic_key     text,
topic_name    text,
group_id       bigint
);

COPY INTO YOUR_DATABASE.MEETUP.MEMBERS_TOPICS(topic_id, topic_key, topic_name, group_id)
FROM (
  SELECT t.$1,t.$2,t.$3,t.$4
  FROM @csvfiles/members_topics.csv.gz t
)
file_format = (format_name = YOUR_DATABASE.MEETUP.FORMAT_TEST);


-- Topics;
CREATE OR REPLACE TABLE YOUR_DATABASE.MEETUP.TOPICS(
topic_id          bigint,
description       text,
link              text,
members           bigint,
topic_name        text,
urlkey            text,
main_topic_id     bigint
);

COPY INTO YOUR_DATABASE.MEETUP.TOPICS(topic_id, description, link, members, topic_name, urlkey, main_topic_id)
FROM (
 SELECT t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7
  FROM @csvfiles/topics.csv.gz t
)
file_format = (format_name = YOUR_DATABASE.MEETUP.FORMAT_TEST, encoding= 'ISO88591');


-- Venues;
CREATE OR REPLACE TABLE YOUR_DATABASE.MEETUP.VENUES(
venue_id                    bigint,
address_1                   text,
city                        varchar(100),
country                     varchar(3),
distance                    numeric,
lat                         numeric,
localized_country_name      varchar(10),
lon                         numeric,
venue_name                  text,
rating                      numeric,
rating_count                numeric,
state                       varchar(10),
zip                         bigint,
normalised_rating           numeric
);

COPY INTO YOUR_DATABASE.MEETUP.VENUES(venue_id, address_1, city, country, distance, lat,
       localized_country_name, lon, venue_name, rating, rating_count,
       state, zip, normalised_rating)
FROM (
  SELECT t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,t.$9,t.$10,
  t.$11,t.$12,t.$13,t.$14
  FROM @csvfiles/venues.csv.gz t
)
file_format = (format_name = YOUR_DATABASE.MEETUP.FORMAT_TEST);