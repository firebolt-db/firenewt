CREATE TABLE "uservisits" ("sourceip" text NOT NULL, 
"destinationurl" text NOT NULL,
"visitdate" pgdate NOT NULL,
"adrevenue" REAL NOT NULL, 
"useragent" text NOT NULL, 
"countrycode" text NOT NULL,
"languagecode" text NOT NULL,
"searchword" text NOT NULL, 
"duration" integer NOT NULL) PRIMARY INDEX "visitdate",
"destinationurl",
"sourceip";

CREATE TABLE "rankings" ("pageurl" text NOT NULL,
"pagerank" integer NULL,
"avgduration" integer NOT NULL) PRIMARY INDEX "pageurl";

CREATE TABLE "ipaddresses" ("ip" text NOT NULL,
"autonomoussystem" integer NOT NULL,
"asname" text NOT NULL) PRIMARY INDEX "ip";

CREATE TABLE "agents" ("id" integer NOT NULL,
"agentname" text NOT NULL,
"operatingsystem" text NOT NULL,
"devicearch" text NOT NULL,
"browser" text NOT NULL);

CREATE TABLE "searchwords" ("word" text NOT NULL,
"word_hash" bigint NOT NULL,
"word_id" bigint NOT NULL,
"firstseen" pgdate NOT NULL,
"is_topic" boolean NOT NULL);

COPY
INTO
	uservisits
FROM
	's3://benchmark-amplab/dataapp/100gb/uservisits/' WITH
TYPE = parquet
CREDENTIALS = (AWS_KEY_ID = '' AWS_SECRET_KEY = '' )
;

COPY
INTO
	rankings
FROM
	's3://benchmark-amplab/dataapp/100gb/rankings/' WITH
TYPE = parquet
CREDENTIALS = (AWS_KEY_ID = '' AWS_SECRET_KEY = '' )
;

COPY
INTO
	ipaddresses
FROM
	's3://benchmark-amplab/dataapp/100gb/dimensions/ipaddresses/' WITH
TYPE = parquet
CREDENTIALS = (AWS_KEY_ID = '' AWS_SECRET_KEY = '' )
;

COPY
INTO
	agents
FROM
	's3://benchmark-amplab/dataapp/100gb/dimensions/agents/' WITH
TYPE = parquet
CREDENTIALS = (AWS_KEY_ID = '' AWS_SECRET_KEY = '' )
;

COPY
INTO
	searchwords
FROM
	's3://benchmark-amplab/dataapp/100gb/dimensions/searchwords/' WITH
TYPE = parquet
CREDENTIALS = (AWS_KEY_ID = '' AWS_SECRET_KEY = '' )
;

VACUUM uservisits;

VACUUM uservisits;

VACUUM rankings;

VACUUM searchwords;

VACUUM agents;

VACUUM ipaddresses;

CREATE aggregating INDEX idx_by_day ON
uservisits (
  visitdate,
  countrycode,
  languagecode,
  useragent,
  MAX(visitdate),
  SUM(adrevenue),
  MAX(adrevenue),
  COUNT(*)
);
