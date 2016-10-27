CREATE TABLE "Data_Warehouse_Facts" (
	"date_id" INT,
	"customer_id" INT,
	"track_id" varchar,
	"geo_area_id" varchar,
	"session_id" varchar,
	"vendor_id" INT,
	constraint DATA_WAREHOUSE_FACTS_PK PRIMARY KEY ("date_id","customer_id","track_id","geo_area_id","session_id")
CREATE sequence "DATA_WAREHOUSE_FACTS_SEQ"
/
CREATE trigger "BI_DATA_WAREHOUSE_FACTS"
  before insert on "DATA_WAREHOUSE_FACTS"
  for each row
begin
  select "DATA_WAREHOUSE_FACTS_SEQ".nextval into :NEW."date_id" from dual;
end;
/

)
/
CREATE TABLE "Customer_Dimension" (
	"customer_id" varchar,
	"anonymized_person_id" INT,
	"refferal_code" varchar,
	"partner_name" varchar,
	"access" varchar,
	"financial_product" varchar,
	"user_product_type" varchar,
	"mobile" BOOLEAN,
	"ingest_datestamp" DATE,
	"applied" INT,
	"gender" varchar,
	"birth_year" INT,
	constraint CUSTOMER_DIMENSION_PK PRIMARY KEY ("customer_id")
CREATE sequence "CUSTOMER_DIMENSION_SEQ"
/
CREATE trigger "BI_CUSTOMER_DIMENSION"
  before insert on "CUSTOMER_DIMENSION"
  for each row
begin
  select "CUSTOMER_DIMENSION_SEQ".nextval into :NEW."customer_id" from dual;
end;
/

)
/
CREATE TABLE "Date_Dimension" (
	"date_id" DATE,
	"day" DATE,
	"local_time" DATE,
	"logtime" varchar,
	"offline_timestamp" varchar,
	constraint DATE_DIMENSION_PK PRIMARY KEY ("date_id")
CREATE sequence "DATE_DIMENSION_SEQ"
/
CREATE trigger "BI_DATE_DIMENSION"
  before insert on "DATE_DIMENSION"
  for each row
begin
  select "DATE_DIMENSION_SEQ".nextval into :NEW."date_id" from dual;
end;
/

)
/
CREATE TABLE "Media_Content_Dimension" (
	"track_id" varchar,
	"isrc" varchar,
	"filemane" varchar,
	"artist_name" varchar,
	"track_name" varchar,
	"album_name" varchar,
	"playlist_id" varchar,
	"playlist_name" varchar,
	"playlist_type" INT,
	"upc" varchar,
	"track_artists" varchar,
	"media_duration" INT,
	"item_type" INT,
	"media_type" INT,
	"publish_date" DATE,
	constraint MEDIA_CONTENT_DIMENSION_PK PRIMARY KEY ("track_id")
CREATE sequence "MEDIA_CONTENT_DIMENSION_SEQ"
/
CREATE trigger "BI_MEDIA_CONTENT_DIMENSION"
  before insert on "MEDIA_CONTENT_DIMENSION"
  for each row
begin
  select "MEDIA_CONTENT_DIMENSION_SEQ".nextval into :NEW."track_id" from dual;
end;
/

)
/
CREATE TABLE "Session_Dimension" (
	"session_id" varchar,
	"track_uri" varchar,
	"srteam_os" varchar,
	"srteam_device" varchar,
	"srteam_cached" varchar,
	"source" varchar,
	"stream_source_uri" varchar,
	"stream_timestamp" INT,
	"stream_start_position" INT,
	"stream_duration" INT,
	"source_of_stream" INT,
	"container_type" INT,
	"offline" INT,
	"action_type" INT,
	"end_reason_type" INT,
	constraint SESSION_DIMENSION_PK PRIMARY KEY ("session_id")
CREATE sequence "SESSION_DIMENSION_SEQ"
/
CREATE trigger "BI_SESSION_DIMENSION"
  before insert on "SESSION_DIMENSION"
  for each row
begin
  select "SESSION_DIMENSION_SEQ".nextval into :NEW."session_id" from dual;
end;
/

)
/
CREATE TABLE "Geographical_Dimension" (
	"geo_area_id" varchar,
	"postal_code" varchar,
	"region_code" varchar,
	"country_code" varchar,
	"utc_offset" varchar,
	constraint GEOGRAPHICAL_DIMENSION_PK PRIMARY KEY ("geo_area_id")
CREATE sequence "GEOGRAPHICAL_DIMENSION_SEQ"
/
CREATE trigger "BI_GEOGRAPHICAL_DIMENSION"
  before insert on "GEOGRAPHICAL_DIMENSION"
  for each row
begin
  select "GEOGRAPHICAL_DIMENSION_SEQ".nextval into :NEW."geo_area_id" from dual;
end;
/

)
/
ALTER TABLE "Data_Warehouse_Facts" ADD CONSTRAINT "Data_Warehouse_Facts_fk0" FOREIGN KEY ("date_id") REFERENCES Date_Dimension("date_id");
ALTER TABLE "Data_Warehouse_Facts" ADD CONSTRAINT "Data_Warehouse_Facts_fk1" FOREIGN KEY ("customer_id") REFERENCES Customer_Dimension("customer_id");
ALTER TABLE "Data_Warehouse_Facts" ADD CONSTRAINT "Data_Warehouse_Facts_fk2" FOREIGN KEY ("track_id") REFERENCES Media_Content_Dimension("track_id");
ALTER TABLE "Data_Warehouse_Facts" ADD CONSTRAINT "Data_Warehouse_Facts_fk3" FOREIGN KEY ("geo_area_id") REFERENCES Geographical_Dimension("geo_area_id");
ALTER TABLE "Data_Warehouse_Facts" ADD CONSTRAINT "Data_Warehouse_Facts_fk4" FOREIGN KEY ("session_id") REFERENCES Session_Dimension("session_id");
