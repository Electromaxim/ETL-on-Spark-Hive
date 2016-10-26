from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql import functions as sqlfunc

# Define source connection parameters
STREAMING_1 = "{streaming service n.1 connection}"
SERVICE_USERNAME = '<USER_NAME >'
SERVICE_PASSWORD = '********'
STREAMING_CONNECTION_URL = "url"

STREAMING_2 = "{streaming service n.2 connection}"
SERVICE_USERNAME = '<USER_NAME >'
SERVICE_PASSWORD = '********'
STREAMING_CONNECTION_URL = "url"

# Define Spark configuration
conf = SparkConf()
conf.setMaster("spark://Box.local:7077")
conf.setAppName("Stream_import")
conf.set("spark.executor.memoryg")

# Initialize a SparkContext and SQLContext
sc = SparkContext(conf=conf)
sql_ctx = SQLContext(sc)

# Initialize hive context
hive_ctx = HiveContext(sc)

# Source 1 Type: Streaming Data
# Table Name   : FILE_1
# # -------------------------------------------------------
# # COLUMN NAME			DATA TYPE
# # -------------------------------------------------------
#day,			date
#log_time,		character varying(20)
#mobile,		boolean
#track_id,		character varying(35)
#isrc,			character varying(20)
#upc, 			character varying(20)
#artist_name,		character varying(255)
#track_name,		character varying(255)
#album_name,		character varying(255)
#customer_id,		character varying(35)
#postal_code,		character varying(15)
#access,		character varying(35)
#country_code,		character varying(2)
#gender, 		character varying(7)
#birth_year,		integer
#filename,		character varying(35)
#region_code,           character varying(50)
#referral_code,		character varying(35)
#partner_name,		character varying(35)
#financial_product,	character varying(35)
#user_product_type, 	character varying(35)
#offline_timestamp,	character varying(35)
#stream_length,		integer
#stream_cached,		character varying(35)
#stream_source,		character varying(35)
#stream_source_uri,	character varying(100)
#stream_device,		character varying(35)
#stream_os, 		character varying(35)
#track_uri,		character varying(100)
#track_artists,         character varying(255)
#source,		character varying(20)
# # -------------------------------------------------------
df_FILE_1 = sql_ctx.load(
    source="KafkaInputDStream",
    path=STREAMING_1,
    driver='spark-streaming-kafka-0-8',
    url=STREAMING_CONNECTION_URL,
    dbtable="FILE_1")

# Source 2 Type : Streaming Data
# Table Name    : FILE_2
# # -------------------------------------------------------
# # COLUMN NAME			DATA TYPE
# # -------------------------------------------------------
#day			date						
#ingest_datestamp  	date						
#appleid		bigint						
#country_code		character varying(10)						
#anonymized_person_id	bigint						
#membership_type	character varying(255)						
#membership_mode	character varying(255)						
#postal_code		character varying(50)						
#device_type		integer			    0=Other,1=mobile,2=desktop
#operating_system	integer			    0 = Other 1 = iOS 2 = Macintosh 3 = Android 4 = Windows 5 = tvOS
#utc_offset		character varying(10)						
#action_type		integer						
#end_reason_type	integer						
#offline		integer			   
#source_of_stream	integer			    0 = Other 1 = My Music 2 = Search 3 = Discovery
#container_type		integer			    0 = Other 1 = Radio station 2 = Playlist 3 = Album
#stream_timestamp	bigint						
#stream_start_position	bigint						
#stream_duration	bigint						
#media_duration		bigint						
#playlist_type		integer						
#playlist_id		character varying(255)						
#playlist_name		character varying(255)						
#local_time		timestamp without time zone						
#isrc			character varying(20)						
#item_type		bigint						
#media_type		bigint			    1 = Music 2 Music Video
#vendor_id		bigint						
#published_date		date			
# # -------------------------------------------------------	
df_FILE_2 = sql_ctx.load(
    source="KafkaInputDStream",
    path=STREAMING_2,
    driver='spark-streaming-kafka-0-8',
    url=STREAMING_CONNECTION_URL,
    dbtable="FILE_2")

# Join the two data frames together on DAY column and eliminate the redundant columns
df_FILE_1_FILE_2_join = df_FILE_1.join(df_FILE_2, "day").select("day", "log_time", "mobile", "track_id", "isrc", "upc", 
"artist_name", "track_name", "album_name", "customer_id", "postal_code", "access", "country_code", "gender", "birth_year",
"filename", "region_code", "referral_code", "partner_name", "financial_product", "user_product_type", "offline_timestamp",
"stream_length", "stream_cached", "stream_source", "stream_source_uri", "stream_device", "stream_os", "track_uri", "track_artists", "source",
"ingest_datestamp", "appleid", "anonymized_person_id", "membership_type", "membership_mode", "device_type",
"operating_system", "utc_offset", "action_type", "end_reason_type", "offline", "source_of_stream", "container_type", "stream_timestamp",
"stream_start_position", "stream_duration", "media_duration", "playlist_type", "playlist_id", "playlist_name", "local_time", "item_type",
"media_type", "vendor_id", "published_date")

# Adding columns to the data frame for partitioning the hive table
df_customer_dim = df_FILE_1_FILE_2_join.withColumn('customer_id', F.customer_id(df_FILE_1_FILE_2_join.published_date))
df_date_dim = df_FILE_1_FILE_2_join.withColumn('date_id', F.date_id(df_FILE_1_FILE_2_join.published_date))
df_media_dim = df_FILE_1_FILE_2_join.withColumn('media_id', F.media_id(df_FILE_1_FILE_2_join.published_date))
df_geo_dim = df_FILE_1_FILE_2_join.withColumn('geo_area_id', F.geo_id(df_FILE_1_FILE_2_join.published_date))
df_session_dim = df_FILE_1_FILE_2_join.withColumn('session_id', F.session_id(df_FILE_1_FILE_2_join.published_date))

# Joining Vendor data with dimensions
df_fact = df_customer_dim.withColumn('vendor_id', F.current_vendor())
df_fact = df_date_dim.withColumn('vendor_id', F.current_vendor())
df_fact = df_media_dim.withColumn('vendor_id', F.current_vendor())
df_fact = df_geo_dim.withColumn('vendor_id', F.current_vendor())
df_fact = df_session_dim.withColumn('vendor_id', F.current_vendor())

df_fact.repartition(5)

# Registering data frame as a temp table for SparkSQL
hive_ctx.registerDataFrameAsTable(df_customer_dim, "MEDIA_TEMP")
hive_ctx.registerDataFrameAsTable(df_date_dim, "MEDIA_TEMP")
hive_ctx.registerDataFrameAsTable(df_media_dim, "MEDIA_TEMP")
hive_ctx.registerDataFrameAsTable(df_geo_dim, "MEDIA_TEMP")
hive_ctx.registerDataFrameAsTable(df_session_dim, "MEDIA_TEMP")
hive_ctx.registerDataFrameAsTable(df_fact, "MEDIA_TEMP")

# Target Type: APACHE HIVE
## Source   : FILE_1, FILE_2
## Table Name : Customer_Dimension
## Storage Format: ORC
## # -------------------------------------------------------
## # COLUMN NAME			DATA TYPE
## # -------------------------------------------------------
#	customer_id 		varchar(35) 	
#	anonymized_person_id 	integer(8) 	
#	refferal_code 		varchar(35) 	
#	partner_name 		varchar(35) 	
#	access 			varchar(35) 	
#	financial_product 	varchar(35) 	
#	user_product_type 	varchar(35) 	
#	mobile 			boolean 	
#	ingest_datestamp 	date 	
#	applied 		integer(8) 	
#	gender 			varchar(7) 	
#	birth_year 		integer(4) 
## # -------------------------------------------------------	
#
## Target Type: APACHE HIVE
## Source   : FILE_1, FILE_2
## Table Name : Date_Dimension
## Storage Format: ORC
## # -------------------------------------------------------
## # COLUMN NAME			DATA TYPE
## # -------------------------------------------------------
#	date_id 		date 	
#	day 			date 	
#	local_time 		date 	
#	logtime 		varchar(20) 	
#	offline_timestamp       varchar(35)
## # -------------------------------------------------------	
#
## Target Type: APACHE HIVE
## Source   : FILE_1, FILE_2
## Table Name : Media_Content_Dimension
## Storage Format: ORC
## # -------------------------------------------------------
## # COLUMN NAME			DATA TYPE
## # -------------------------------------------------------
#	media_id 	    varchar(35) 	
#	track_id 	    varchar(35) 	
#	ISRC 		    varchar(20) 	
#	filemane 	    varchar(255) 	
#	artist_name 	    varchar(255) 	
#	track_name 	    varchar(255) 	
#	album_name 	    varchar(255) 	
#	playlist_id 	    varchar(255) 	
#	playlist_name 	    varchar(255) 	
#	playlist_type 	    integer(4) 	
#	upc 		    varchar(20) 	
#	track_artists 	    varchar(255) 	
#	media_duration 	    integer(8) 	
#	item_type 	    integer(8) 	
#	media_type 	    integer(8) 	
#	publish_date 	    date  
## # -------------------------------------------------------	
#
## Target Type: APACHE HIVE
## Source   : FILE_1, FILE_2
## Table Name : Session_Dimension
## Storage Format: ORC
## # -------------------------------------------------------
## # COLUMN NAME			DATA TYPE
## # -------------------------------------------------------
#	session_id 		varchar(35) 	
#	srteam_os 		varchar(35) 	
#	srteam_device 		varchar(35) 	
#	srteam_cached 		varchar(35) 	
#	source 			varchar(20) 	
#	track_uri 		varchar(100) 	
#	stream_source_uri 	varchar(100) 	
#	stream_timestamp 	integer(8) 	
#	stream_start_position 	integer(8) 	
#	stream_duration 	integer(8) 	
#	source_of_stream 	integer(4) 	
#	container_type 		integer(4) 	
#	offline 		integer(4) 	
#	action_type 		integer(4) 	
#	end_reason_type 	integer(4) 	
#	stream_length 		integer(4) 
## # -------------------------------------------------------	
#
## Target Type: APACHE HIVE
## Source   : FILE_1, FILE_2
## Table Name : Geographical_Dimension
## Storage Format: ORC
## # -------------------------------------------------------
## # COLUMN NAME			DATA TYPE
## # -------------------------------------------------------
#	geo_area_id 		varchar(35) 	
#	country_code 		varchar(10) 	
#	utc_offset 		varchar(10) 	
#	postal_code		varchar(15) 	
#	region_code 		varchar(50)
## # -------------------------------------------------------	
#
## Target Type: APACHE HIVE
## Source   : FILE_1, FILE_2
## Table Name : Data_Warehouse_Facts
## Storage Format: ORC
## # -------------------------------------------------------
## # COLUMN NAME			DATA TYPE
## # -------------------------------------------------------
#	date_id 		integer(8) 	
#	customer_id 		integer(8) 	
#	media_id 		varchar(35) 	
#	geo_area_id 		varchar(35) 	
#	session_id 		varchar(35) 	
#	vendor_id 		integer(8)  
## # -------------------------------------------------------	
#

# Insert transformed data into the Target table
hive_ctx.sql("INSERTWRITE TABLE KafkaInputDStream.Customer_Dimension PARTITION (customer_id)     \
              INSERTWRITE TABLE KafkaInputDStream.Date_Dimension PARTITION (date_id)             \
              INSERTWRITE TABLE KafkaInputDStream.Media_Content_Dimension PARTITION (media_id)   \
              INSERTWRITE TABLE KafkaInputDStream.Session_Dimension PARTITION (session_id)       \
              INSERTWRITE TABLE KafkaInputDStream.Geographical_Dimension PARTITION (geo_area_id)  \
              INSERTWRITE TABLE KafkaInputDStream.Data_Warehouse_Facts PARTITION (fact_id)         \
              SELECT "day", "log_time", "mobile", "track_id", "isrc", "upc", "artist_name", "track_name", "album_name", "customer_id", "postal_code", "access", "country_code", "gender", "birth_year",
"filename", "region_code", "referral_code", "partner_name", "financial_product", "user_product_type", "offline_timestamp",
"stream_length", "stream_cached", "stream_source", "stream_source_uri", "stream_device", "stream_os", "track_uri", "track_artists", "source",
"ingest_datestamp", "appleid", "anonymized_person_id", "membership_type", "membership_mode", "device_type",
"operating_system", "utc_offset", "action_type", "end_reason_type", "offline", "source_of_stream", "container_type", "stream_timestamp",
"stream_start_position", "stream_duration", "media_duration", "playlist_type", "playlist_id", "playlist_name", "local_time", "item_type",
"media_type", "vendor_id", "published_date" FROM MEDIA_TEMP")