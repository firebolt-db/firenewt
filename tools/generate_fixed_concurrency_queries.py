queries_templates = {
    "app_q1": {
        "query_start_ts": "2023-01-01 11:00:49",
        "query_end_ts": "2023-01-01 11:00:50",
        "generator_sql": """SELECT 'SELECT visitdate, sourceip, adrevenue 
FROM uservisits 
WHERE (visitdate BETWEEN '''|| visitdate || ''' AND ''' || (visitdate + 14) || ''')
AND sourceip IN ('''|| sourceip || ''')
AND countrycode = '''|| countrycode || ''';' AS QUERY_TEXT
from
(SELECT 
 		sourceip, 
    ANY_VALUE(countrycode) as countrycode, 
    visitdate
 FROM uservisits
 WHERE 
  (visitdate, sourceip) in 
    (SELECT  
        visitdate,  		      
  		ANY_VALUE(sourceip)        
    FROM 
        uservisits
    GROUP BY ALL
    limit 320)
 GROUP BY ALL);""",
    },
    "app_q2": {
        "query_start_ts": "2023-01-01 11:00:59",
        "query_end_ts": "2023-01-01 11:01:00",
        "generator_sql": """SELECT 'with desktop as ( 
            select 
            date_trunc(''month'', visitdate) as year_month_day,
            sourceip,
            countrycode, 
            count(*) as visits,
            sum(adrevenue) as adrevenue, 
            count(distinct languagecode)  as languagecode, 
            max(length(searchword)) as searchwordlength,
            sum(duration) as time_on_site
            from uservisits
            where sourceip in ('''|| sourceip_1 || ''')
            and countrycode < ''zzz''
            and visitdate between ''' || visitdate || ''' and ''' || (visitdate + 5) || '''
            and useragent = '''|| useragent_1 || '''
            group by sourceip,countrycode,year_month_day), 
 mobile as (
            select 
            date_trunc(''month'', visitdate) as year_month_day,
            sourceip,
            countrycode, 
            count(*) as visits,
            sum(adrevenue) as adrevenue, 
            count(distinct languagecode)  as languagecode, 
            max(length(searchword)) as searchwordlength,
            sum(duration) as time_on_site
            from uservisits
            where sourceip in ('''|| sourceip_2 || ''')
            and countrycode < ''zzz''
            and visitdate between ''' || visitdate || ''' and ''' || (visitdate + 5) || '''
            and useragent = '''|| useragent_2 || '''
            group by sourceip,countrycode,year_month_day)
select 
    COALESCE(desktop.year_month_day,mobile.year_month_day) as year_month_day,
    COALESCE(desktop.sourceip,mobile.sourceip) as site,
    COALESCE(desktop.countrycode,mobile.countrycode) as countrycode,
    COALESCE(desktop.visits,0) + COALESCE(mobile.visits,0) as visits,
    COALESCE(desktop.adrevenue,0)+ COALESCE(mobile.adrevenue,0) as page_views,
    COALESCE(desktop.searchwordlength,0) + COALESCE(mobile.searchwordlength,0) as searchwordlength,
    COALESCE(desktop.time_on_site,0) + COALESCE(mobile.time_on_site,0) as time_on_site
from desktop
full outer join mobile on 
mobile.year_month_day = desktop.year_month_day AND
mobile.sourceip = desktop.sourceip AND
mobile.countrycode = desktop.countrycode
;' AS QUERY_TEXT
from
(select max(sourceip) as sourceip_1, 
        max_by(useragent, sourceip) as useragent_1, 
		    min(sourceip) as sourceip_2, 
        min_by(useragent, sourceip) as useragent_2,   
        visitdate
FROM uservisits
group by all
limit 320);""",
    },
    "app_q3": {
        "query_start_ts": "2023-01-01 11:01:09",
        "query_end_ts": "2023-01-01 11:01:10",
        "generator_sql": """SELECT 'SELECT languagecode,
    MAX(visitdate) AS visitdate,
    ARRAY_JOIN(ARRAY_AGG(countrycode), '','') AS countrycode
FROM uservisits
WHERE sourceip = '''|| sourceip || ''' and visitdate between ''' || visitdate || ''' and ''' || (visitdate + 1) || '''
GROUP BY languagecode;' AS QUERY_TEXT
from
(select any(sourceip) as sourceip, visitdate
FROM uservisits
group by all
limit 320);""",
    },
    "app_q4": {
        "query_start_ts": "2023-01-01 11:01:19",
        "query_end_ts": "2023-01-01 11:01:20",
        "generator_sql": """SELECT 'SELECT *
FROM uservisits
WHERE sourceip = '''|| sourceip || ''' and visitdate between ''' || visitdate || ''' and ''' || (visitdate + 1) || ''';' AS QUERY_TEXT
from
(select any(sourceip) as sourceip, visitdate
FROM uservisits
group by all
limit 320);""",
    },
    "app_q5": {
        "query_start_ts": "2023-01-01 11:01:29",
        "query_end_ts": "2023-01-01 11:01:30",
        "generator_sql": """SELECT 'WITH filtered_uservisits AS( SELECT *     
  FROM uservisits     
  WHERE visitdate between '''|| visitdate || ''' and '''|| (visitdate+1) || ''') 
  SELECT allowed_records.destinationurl, COUNT(*) OVER () as total_uservisits_count 
  FROM filtered_uservisits JOIN ( SELECT destinationurl 
  							FROM filtered_uservisits 
  							ORDER BY duration DESC LIMIT 100 ) AS allowed_records ON allowed_records.destinationurl = filtered_uservisits.destinationurl 
  ORDER BY adrevenue desc LIMIT 20 OFFSET 0;' AS QUERY_TEXT
from   
(select distinct visitdate
FROM uservisits
group by all
limit 320);
""",
    },
    "app_q6": {
        "query_start_ts": "2023-01-01 11:01:39",
        "query_end_ts": "2023-01-01 11:01:40",
        "generator_sql": """SELECT 'SELECT COUNT(*) as c FROM uservisits WHERE sourceip = '''|| sourceip || ''' and visitdate = '''|| visitdate || ''';'
from
(select any(sourceip) as sourceip, visitdate
FROM uservisits
group by all
limit 320);""",
    },
    "app_q7": {
        "query_start_ts": "2023-01-01 11:01:49",
        "query_end_ts": "2023-01-01 11:01:50",
        "generator_sql": """SELECT 'SELECT max(visitdate) as latest_visit
FROM uservisits
WHERE (visitdate >= '''|| visitdate ||''' AND visitdate <= DATE_ADD(''DAY'', 1, '''|| visitdate || '''));' AS QUERY_TEXT
from
(select distinct visitdate
FROM uservisits
group by all
limit 320);""",
    },
    "app_q8": {
        "query_start_ts": "2023-01-01 11:01:59",
        "query_end_ts": "2023-01-01 11:02:00",
        "generator_sql": """
        with a as materialized (select * from (select distinct visitdate
FROM uservisits
limit 1000) a cross join  
(select array_agg(languagecode) languagecodes
  from
(select distinct languagecode
from uservisits)))  
        SELECT 'SELECT date_trunc(''month'', visitdate) as year_month_day,
COALESCE(SUM(duration), 0) as installs,
COALESCE(SUM(length(searchword)), 0) as billingCost,
SUM(CASE WHEN adrevenue <= 1.5 THEN duration ELSE 0 END) as revenueD7
FROM uservisits
WHERE (visitdate >= '''|| visitdate ||''' AND visitdate <= DATE_ADD(''DAY'', 1, '''|| visitdate ||'''))
        AND languagecode IN ('''||languagecodes[((RANDOM()+0.5)*60)::int]||''','''||languagecodes[((RANDOM()+0.5)*60)::int]||''','''||languagecodes[((RANDOM()+0.5)*60)::int]||
  ''','''||languagecodes[((RANDOM()+0.5)*60)::int]||''','''||languagecodes[((RANDOM()+0.5)*60)::int]||''','''||languagecodes[((RANDOM()+0.5)*60)::int]||
  ''','''||languagecodes[((RANDOM()+0.5)*60)::int]||''','''||languagecodes[((RANDOM()+0.5)*60)::int]||''')
GROUP BY 1
ORDER BY 1;' as query_text
  from a
  limit 320;""",
    },
    "app_q9": {
        "query_start_ts": "2023-01-01 11:02:09",
        "query_end_ts": "2023-01-01 11:02:10",
        "generator_sql": """SELECT 'select destinationurl
from uservisits
where adrevenue between '||0.3*RANDOM()||' and '||RANDOM()||' and visitdate between '''|| visitdate ||''' and '''|| visitdate+6 ||'''
group by destinationurl having count(*) > 100;' AS QUERY_TEXT
  from
(select distinct visitdate
FROM uservisits
limit 320) """,
    },
    "app_q10": {
        "query_start_ts": "2023-01-01 11:02:15",
        "query_end_ts": "2023-01-01 11:02:16",
        "generator_sql": """SELECT 'with busiest_days as (
  select visitdate, count(*)
  from uservisits
  group by 1
  order by 2 desc
  limit '||(600*RANDOM())::int||'
)
select countrycode, avg(length(searchword))
from uservisits
where visitdate in (select visitdate from busiest_days)
group by countrycode;' as query_text
  from (select visitdate
FROM uservisits
limit 320) """
    },
    "app_q11": {
        "query_start_ts": "2023-01-01 11:02:19",
        "query_end_ts": "2023-01-01 11:02:20",
        "generator_sql": """with a as materialized (select * from (select distinct visitdate
FROM uservisits
limit 1000) a cross join  
(select array_agg(searchword) searchwords, countrycode
  from
(select distinct searchword, countrycode from uservisits)
  group by all)
  )  
        SELECT 'SELECT searchword, useragent, languagecode
from uservisits 
where countrycode = '''||countrycode||'''
  and visitdate = '''||visitdate||'''
  and searchword in ('''||searchwords[((RANDOM()+0.5)*60)::int]||''','''||searchwords[((RANDOM()+0.5)*60)::int]||''','''||searchwords[((RANDOM()+0.5)*60)::int]||''','''||searchwords[((RANDOM()+0.5)*60)::int]||''','''||searchwords[((RANDOM()+0.5)*60)::int]||''','''||searchwords[((RANDOM()+0.5)*60)::int]||''')
Limit 65;' as query_text
  from a
  limit 320;""",
    },
    "app_q12": {
        "query_start_ts": "2023-01-01 11:02:29",
        "query_end_ts": "2023-01-01 11:02:30",
        "generator_sql": """WITH random_visitdates AS (
    SELECT visitdate
    FROM uservisits
    GROUP BY visitdate
    ORDER BY RANDOM()
    limit 320
),
random_rows AS (
    SELECT
        visitdate,
        countrycode,
        languagecode,
        useragent,
        ROW_NUMBER() OVER (PARTITION BY visitdate ORDER BY RANDOM()) AS rn
    FROM uservisits
    WHERE visitdate IN (SELECT visitdate FROM random_visitdates)
),
filtered_rows AS (
    SELECT
        visitdate,
        ARRAY_AGG(countrycode) AS countrycodes,
        ARRAY_AGG(languagecode) AS languagecodes,
        ARRAY_AGG(useragent) AS useragents
    FROM random_rows
    WHERE rn <= 10
    GROUP BY visitdate
)
SELECT 'SELECT
  countrycode,
  languagecode,
  COUNT(DISTINCT visitdate) AS days_with_data,
  MAX(visitdate) last_visit,
  SUM(adrevenue) sum_adrevenue,
  MAX(adrevenue) max_adrevenue,
  COUNT(*) cnt
FROM
  uservisits
WHERE
  visitdate >= '''||visitdate||''' and visitdate <= '''||(visitdate+30)||''' AND
  countrycode in ( '''||ARRAY_TO_STRING(countrycodes,''',''')||''') AND
  languagecode in ( '''||ARRAY_TO_STRING(languagecodes,''',''')||''') AND
  useragent in ( '''||ARRAY_TO_STRING(useragents,''',''')||''')
GROUP BY 1, 2;' as query_text
  from filtered_rows""",
    },
    "app_q13": {
        "query_start_ts": "2023-01-01 11:02:31",
        "query_end_ts": "2023-01-01 11:02:32",
        "generator_sql": """SELECT 'WITH                 
                CTE1 AS 
                ( 
                    SELECT languagecode FROM uservisits
WHERE uservisits.countrycode = '''||countrycode_1||'''
  limit 1
                ),
                CTE2 AS 
                ( 
                    SELECT uservisits.languagecode, 
                    null AS topic, 
                    SUM(CASE WHEN agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname  THEN duration ELSE NULL END) 
                    AS s1, 
                    SUM(CASE WHEN agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname  THEN duration ELSE NULL END) 
                    AS s2, 
                    SUM(CASE WHEN agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname  THEN duration ELSE NULL END) 
                    AS s3 
                    FROM uservisits

 INNER JOIN agents ON  uservisits.useragent = agents.agentname 

                    WHERE 
                    uservisits.visitdate between '''||visitdate||'''::DATE and '''||to_date(visitdate + interval '1 month')||'''::DATE AND
                    uservisits.countrycode IN ('''||countrycode_1||''',''URY'',''MKD'') AND uservisits.searchword IN ('''||REPLACE(REPLACE(searchword_1, '''', ''''''), '"', '\"')||''') 
                    AND agents.operatingsystem = ''Windows 10''
                    GROUP BY uservisits.languagecode
                ), CTE3 AS 
                ( 
                    SELECT uservisits.languagecode, null AS topic, SUM(CASE WHEN agents.operatingsystem = ''Windows 10'' 
                    AND uservisits.useragent = agents.agentname  THEN duration ELSE NULL END) AS s1, 
                    SUM(CASE WHEN agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname  THEN duration ELSE NULL END) 
                    AS s4, 
                    SUM(CASE WHEN agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname  THEN duration ELSE NULL END) AS s2, 
                    SUM(CASE WHEN agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname  THEN duration ELSE NULL END) AS s3 
                    FROM uservisits

 INNER JOIN agents ON  uservisits.useragent = agents.agentname 

                    WHERE 
                    uservisits.visitdate between '''||visitdate||'''::DATE and '''||to_date(visitdate + interval '1 month')||'''::DATE AND
                    uservisits.countrycode IN ('''||countrycode_1||''',''URY'',''MKD'') AND uservisits.searchword IN ('''||REPLACE(REPLACE(searchword_1, '''', ''''''), '"', '\"')||''') 
                    AND agents.operatingsystem = ''Windows 10''
                    GROUP BY uservisits.languagecode
                ), CTE4 AS 
                ( 
                    SELECT uservisits.languagecode, null AS topic, 
                    SUM(CASE WHEN agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname  THEN duration ELSE NULL END) AS s1, 
                    SUM(CASE WHEN agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname  THEN duration ELSE NULL END) AS 
                    s5, SUM(CASE WHEN agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname  THEN duration ELSE NULL END) AS s2, 
                    SUM(CASE WHEN agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname  THEN duration ELSE NULL END) AS s3 
                    FROM uservisits

 INNER JOIN agents ON  uservisits.useragent = agents.agentname 

                    WHERE 
                    uservisits.visitdate between '''||visitdate||'''::DATE and '''||to_date(visitdate + interval '1 month')||'''::DATE AND
                    uservisits.countrycode IN ('''||countrycode_1||''',''URY'',''MKD'') 
                    AND uservisits.searchword IN ('''||REPLACE(REPLACE(searchword_1, '''', ''''''), '"', '\"')||''') 
                    AND agents.operatingsystem = ''Windows 10''
                    GROUP BY uservisits.languagecode
                ), CTE5 AS 
                ( 
                    SELECT uservisits.languagecode, null AS topic, 
                    (COUNT(DISTINCT CASE WHEN ((case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname  
                        THEN 1 ELSE NULL END) * 1) = (case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname THEN 1 ELSE NULL END) THEN languagecode ELSE NULL END)) AS s1, 
                    (COUNT(DISTINCT CASE WHEN ((case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname  
                        THEN 1 ELSE NULL END) * 1) = (case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname THEN 1 ELSE NULL END) THEN languagecode ELSE NULL END)) AS s5, 
                    (COUNT(DISTINCT CASE WHEN ((case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname  
                        THEN 1 ELSE NULL END) * 1) = (case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname THEN 1 ELSE NULL END) THEN languagecode ELSE NULL END)) AS s4 
                    FROM uservisits

 INNER JOIN agents ON  uservisits.useragent = agents.agentname 

                    WHERE 
                    uservisits.visitdate between '''||visitdate||'''::DATE and '''||to_date(visitdate + interval '1 month')||'''::DATE AND
                    uservisits.countrycode IN ('''||countrycode_1||''',''URY'',''MKD'') 
                    AND agents.operatingsystem = ''Windows 10''
                    GROUP BY uservisits.languagecode
                ), CTE6 AS 
                ( 
                    SELECT uservisits.languagecode, null AS topic, 
                    (COUNT(DISTINCT CASE WHEN ((case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname  
                        THEN 1 ELSE NULL END) * 1) = (case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname THEN 1 ELSE NULL END) THEN languagecode ELSE NULL END)) AS s3 
                    FROM uservisits
   INNER JOIN searchwords ON uservisits.searchword = searchwords.word
 INNER JOIN agents ON uservisits.useragent = agents.agentname 

                    WHERE 
                    uservisits.visitdate between '''||to_date(visitdate + interval '2 month')||'''::DATE and '''||to_date(visitdate + interval '3 month')||'''::DATE AND
                    uservisits.countrycode IN ('''||countrycode_1||''',''URY'',''MKD'') AND languagecode NOT IN ( SELECT * FROM CTE1 ) 
                    AND uservisits.sourceip IN (''123.143.30.99'', ''126.98.46.113'') 
                    AND agents.operatingsystem = ''macOS''
                    GROUP BY uservisits.languagecode
                ), CTE7 AS 
                ( 
                    SELECT uservisits.languagecode, null AS topic,
                    (COUNT(DISTINCT CASE WHEN ((case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname  
                        THEN 1 ELSE NULL END) * 1) = (case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname THEN 1 ELSE NULL END) THEN 
                        CASE WHEN languagecode IS NOT NULL 
                        AND uservisits.sourceip <> ''118.113.25.140'' THEN languagecode ELSE NULL END END)) 
                        AS s6 
                    FROM uservisits

 INNER JOIN agents ON uservisits.useragent = agents.agentname 

                    WHERE 
                    uservisits.visitdate between '''||to_date(visitdate + interval '5 month')||'''::DATE and '''||to_date(visitdate + interval '6 month')||'''::DATE AND
                    uservisits.countrycode IN ('''||countrycode_1||''',''URY'',''MKD'') 
                    AND agents.devicearch = ''ARM''
                    GROUP BY uservisits.languagecode
                ), CTE8 AS 
                ( 
                    SELECT uservisits.languagecode, null AS topic, 
                    (COUNT(DISTINCT CASE WHEN ((case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname  
                        THEN 1 ELSE NULL END) * 1) = (case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname 
                        THEN 1 ELSE NULL END) THEN languagecode ELSE NULL END)) AS s6 
                    FROM uservisits

 INNER JOIN agents ON uservisits.useragent = agents.agentname 

                    WHERE 
                    uservisits.visitdate between '''||to_date(visitdate + interval '5 month')||'''::DATE and '''||to_date(visitdate + interval '6 month')||'''::DATE AND
                    uservisits.countrycode IN ('''||countrycode_1||''',''URY'',''MKD'') 
                    AND agents.devicearch = ''x86''
                    GROUP BY uservisits.languagecode
                ), CTE9 AS 
                ( 
                    SELECT uservisits.languagecode, null AS topic, 
                    (COUNT(DISTINCT CASE WHEN ((case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname 
                        THEN 1 ELSE NULL END) * 1) = (case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname THEN 1 ELSE NULL END) 
                        THEN CASE WHEN languagecode IS NOT NULL 
                        AND uservisits.sourceip <> ''118.113.25.140'' THEN languagecode ELSE NULL END END)) AS s6 
                    FROM uservisits

 INNER JOIN agents ON uservisits.useragent = agents.agentname 

                    WHERE 
                    uservisits.visitdate between '''||to_date(visitdate + interval '5 month')||'''::DATE and '''||to_date(visitdate + interval '6 month')||'''::DATE AND
                    uservisits.countrycode IN ('''||countrycode_1||''',''URY'',''MKD'') 
                    AND agents.devicearch = ''x86''
                    GROUP BY uservisits.languagecode
                ), CTE10 AS 
                ( 
                    SELECT uservisits.languagecode, null AS topic, 
                    (COUNT(DISTINCT CASE WHEN ((case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname 
                        THEN 1 ELSE NULL END) * 1) = (case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname 
                        THEN 1 ELSE NULL END) THEN languagecode ELSE NULL END)) AS s6 
                    FROM uservisits

 INNER JOIN agents ON uservisits.useragent = agents.agentname 

                    WHERE 
                    uservisits.visitdate between '''||to_date(visitdate + interval '5 month')||'''::DATE and '''||to_date(visitdate + interval '6 month')||'''::DATE AND
                    uservisits.countrycode IN ('''||countrycode_1||''',''URY'',''MKD'') 
                    AND agents.devicearch = ''ARM''
                    GROUP BY uservisits.languagecode
                ), CTE11 AS 
                ( 
                    SELECT uservisits.languagecode, null AS topic, 
                    (COUNT(DISTINCT CASE WHEN ((case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname 
                        THEN 1 ELSE NULL END) * 1) = (case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname THEN 1 ELSE NULL END) THEN 
                        CASE WHEN languagecode IS NOT NULL 
                        AND uservisits.sourceip <> ''118.113.25.140'' THEN languagecode ELSE NULL END END)) AS s6 
                    FROM uservisits

 INNER JOIN agents ON uservisits.useragent = agents.agentname 

                    WHERE 
                    uservisits.visitdate between '''||to_date(visitdate + interval '5 month')||'''::DATE and '''||to_date(visitdate + interval '6 month')||'''::DATE AND
                    uservisits.countrycode IN ('''||countrycode_1||''',''URY'',''MKD'') 
                    AND agents.devicearch = ''x86''
                    GROUP BY uservisits.languagecode
                ), CTE12 AS 
                ( 
                    SELECT uservisits.languagecode, null AS topic, 
                    (COUNT(DISTINCT CASE WHEN ((case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname 
                        THEN 1 ELSE NULL END) * 1) = (case when agents.operatingsystem = ''Windows 10'' AND uservisits.useragent = agents.agentname THEN 1 ELSE NULL END) THEN
                        languagecode ELSE NULL END)) AS s6 
                    FROM uservisits

 INNER JOIN agents ON uservisits.useragent = agents.agentname 

                    WHERE 
                    uservisits.visitdate between '''||to_date(visitdate + interval '5 month')||'''::DATE and '''||to_date(visitdate + interval '6 month')||'''::DATE AND
                    uservisits.countrycode IN ('''||countrycode_1||''',''URY'',''MKD'') 
                    AND agents.devicearch = ''ARM''
                    GROUP BY uservisits.languagecode
                ), 
                CTE13 AS 
                ( 
                    SELECT CTE2.languagecode, CTE2.topic, (((COALESCE(CTE2.s1,0)*1.0)/8)-(((COALESCE(CTE3.s1,0)*1.0)/8)+((COALESCE(CTE4.s1,0)*1.0)/8)))/(CTE5.s1) AS s1 
                    FROM CTE2
LEFT JOIN  CTE3 ON CTE2.languagecode = CTE3.languagecode 
LEFT JOIN  CTE4 ON CTE2.languagecode = CTE4.languagecode 
LEFT JOIN  CTE5 ON CTE2.languagecode = CTE5.languagecode 
                ), CTE14 AS 
                ( 
                    SELECT CTE4.languagecode, CTE4.topic, (((COALESCE(CTE4.s5,0)*1.0)/8))/(CTE5.s5) AS s5 
                    FROM CTE4
LEFT JOIN  CTE5 ON CTE4.languagecode = CTE5.languagecode 
                ), CTE15 AS 
                ( 
                    SELECT CTE3.languagecode, CTE3.topic, (((COALESCE(CTE3.s4,0)*1.0)/8))/(CTE5.s4) AS s4 
                    FROM CTE3
LEFT JOIN  CTE5 ON CTE3.languagecode = CTE5.languagecode 
                ), CTE16 AS 
                ( 
                    SELECT CTE2.languagecode, CTE2.topic, CASE WHEN (((COALESCE(CTE2.s2,0)*1.0)/8)-((COALESCE(CTE4.s2,0)*1.0)/8)) <>0 THEN 100*(((COALESCE(CTE2.s2,0)*1.0)/8)-(((COALESCE(CTE3.s2,0)*1.0)/8)+((COALESCE(CTE4.s2,0)*1.0)/8)))/(((COALESCE(CTE2.s2,0)*1.0)/8)-((COALESCE(CTE4.s2,0)*1.0)/8)) ELSE 0 END AS s2 
                    FROM CTE2
LEFT JOIN  CTE3 ON CTE2.languagecode = CTE3.languagecode 
LEFT JOIN  CTE4 ON CTE2.languagecode = CTE4.languagecode 
                ), CTE17 AS 
                ( 
                    SELECT CTE6.languagecode, CTE6.topic, CASE WHEN (((COALESCE(CTE2.s3,0)*1.0)/8)-(((COALESCE(CTE3.s3,0)*1.0)/8)+((COALESCE(CTE4.s3,0)*1.0)/8)))<>0 THEN (CTE6.s3)/(((COALESCE(CTE2.s3,0)*1.0)/8)-(((COALESCE(CTE3.s3,0)*1.0)/8)+((COALESCE(CTE4.s3,0)*1.0)/8))) ELSE 0 END AS s3 
                    FROM CTE6
LEFT JOIN  CTE2 ON CTE6.languagecode = CTE2.languagecode 
LEFT JOIN  CTE3 ON CTE6.languagecode = CTE3.languagecode 
LEFT JOIN  CTE4 ON CTE6.languagecode = CTE4.languagecode 
                ), CTE18 AS 
                ( 
                    SELECT CTE7.languagecode, CTE7.topic, (((CASE WHEN (COALESCE(CTE8.s6,0)*1.0)<>0 THEN (COALESCE(CTE7.s6,0)*1.0)/(COALESCE(CTE8.s6,0)*1.0) ELSE 0 END)+(CASE WHEN (COALESCE(CTE10.s6,0)*1.0)<>0 THEN (COALESCE(CTE9.s6,0)*1.0)/(COALESCE(CTE10.s6,0)*1.0)ELSE 0 END)+(CASE WHEN (COALESCE(CTE12.s6,0)*1.0) <>0 THEN (COALESCE(CTE11.s6,0)*1.0)/(COALESCE(CTE12.s6,0)*1.0)ELSE 0 END ))/3)*100 AS s6 
                    FROM CTE7
LEFT JOIN  CTE8 ON CTE7.languagecode = CTE8.languagecode 
LEFT JOIN  CTE9 ON CTE7.languagecode = CTE9.languagecode 
LEFT JOIN  CTE10 ON CTE7.languagecode = CTE10.languagecode 
LEFT JOIN  CTE11 ON CTE7.languagecode = CTE11.languagecode 
LEFT JOIN  CTE12 ON CTE7.languagecode = CTE12.languagecode 
                ), 
                CTE19 AS 
                ( 
                    SELECT CAST(languagecode AS VARCHAR(1000)) AS languagecode,
                    topic, 
                    MAX(s1) AS s1, MAX(s5) AS s5, MAX(s4) AS s4, MAX(s2) AS s2, MAX(s3) AS s3, MAX(s6) AS s6 
                    FROM 
(

SELECT
CTE13.languagecode, CTE13.topic, CTE13.s1, NULL AS s5, NULL AS s4, NULL AS s2, NULL AS s3, NULL AS s6
FROM
CTE13
UNION ALL

SELECT
CTE14.languagecode, CTE14.topic, NULL AS s1, CTE14.s5, NULL AS s4, NULL AS s2, NULL AS s3, NULL AS s6
FROM
CTE14
UNION ALL

SELECT
CTE15.languagecode, CTE15.topic, NULL AS s1, NULL AS s5, CTE15.s4, NULL AS s2, NULL AS s3, NULL AS s6
FROM
CTE15
UNION ALL

SELECT
CTE16.languagecode, CTE16.topic, NULL AS s1, NULL AS s5, NULL AS s4, CTE16.s2, NULL AS s3, NULL AS s6
FROM
CTE16
UNION ALL

SELECT
CTE17.languagecode, CTE17.topic, NULL AS s1, NULL AS s5, NULL AS s4, NULL AS s2, CTE17.s3, NULL AS s6
FROM
CTE17
UNION ALL

SELECT
CTE18.languagecode, CTE18.topic, NULL AS s1, NULL AS s5, NULL AS s4, NULL AS s2, NULL AS s3, CTE18.s6
FROM
CTE18
) T
GROUP BY languagecode, topic                    
                )              
SELECT *
FROM CTE19;' as query_text
from
(select visitdate, ANY(countrycode) countrycode_1, ANY(searchword) searchword_1
  from 
(select distinct visitdate, countrycode, searchword
  from uservisits)
  group by all
  limit 320)  """,
    },
    "app_q14": {
        "query_start_ts": "2023-01-01 11:02:39",
        "query_end_ts": "2023-01-01 11:02:40",
        "generator_sql": """SELECT 'SELECT
    s.is_topic,
    COALESCE(COUNT(DISTINCT uv.sourceip), 0) AS t1visits
FROM
    uservisits uv
LEFT JOIN rankings r ON (coalesce(uv.destinationurl,''/'')) = r.pageurl
LEFT JOIN ipaddresses i ON (coalesce(uv.sourceip,''0.0.0.0'')) = i.ip
LEFT JOIN agents a ON uv.useragent = a.agentname
LEFT JOIN searchwords s ON uv.searchword = s.word
WHERE
  a.operatingsystem = ''macOS'' AND 
  uv.visitdate >= '''||visitdate||''' AND uv.visitdate < '''||(visitdate + 31)||'''
    AND coalesce(uv.countrycode, '''') = '''||countrycode||'''
    AND (
        CASE
            WHEN uv.countrycode = '''' AND uv.sourceip IS NOT NULL THEN ''Populated''
            WHEN uv.countrycode = '''' THEN ''Not Populated''
            ELSE ''Populated''
        END = ''Populated''
    )
    AND (CASE WHEN (CASE
            WHEN ''Off'' = ''Off'' THEN TRUE
            WHEN ''Date'' = ''Date'' AND ''Off'' = ''Complete'' AND EXTRACT(HOUR FROM CURRENT_TIMESTAMP) >= 9 THEN (uv.visitdate + interval ''1'' day > CURRENT_DATE)
            WHEN ''Date'' = ''Week'' AND ''Off'' = ''Complete'' AND EXTRACT(HOUR FROM CURRENT_TIMESTAMP) >= 9 THEN uv.visitdate >= date_trunc(''week'', CURRENT_DATE) - interval ''1 week'' 
            WHEN ''Date'' = ''Month'' AND ''Off'' = ''Complete'' AND EXTRACT(HOUR FROM CURRENT_TIMESTAMP) >= 9 THEN uv.visitdate >= date_trunc(''month'', CURRENT_DATE) - interval ''1 month'' 
            WHEN ''Date'' = ''Date'' AND ''Off'' = ''Partial'' AND EXTRACT(HOUR FROM CURRENT_TIMESTAMP) >= 9 THEN (uv.visitdate + interval ''1'' day > CURRENT_DATE)
            WHEN ''Date'' = ''Week'' AND ''Off'' = ''Partial'' AND EXTRACT(HOUR FROM CURRENT_TIMESTAMP) >= 9 THEN uv.visitdate >= date_trunc(''week'', CURRENT_DATE) - interval ''1 week'' 
            WHEN ''Date'' = ''Month'' AND ''Off'' = ''Partial'' AND EXTRACT(HOUR FROM CURRENT_TIMESTAMP) >= 9 THEN uv.visitdate >= date_trunc(''month'', CURRENT_DATE) - interval ''1 month'' 
            WHEN ''Date'' = ''Date'' AND ''Off'' = ''Complete'' AND EXTRACT(HOUR FROM CURRENT_TIMESTAMP) < 9 THEN (uv.visitdate + interval ''1'' day > CURRENT_DATE)
            WHEN ''Date'' = ''Week'' AND ''Off'' = ''Complete'' AND EXTRACT(HOUR FROM CURRENT_TIMESTAMP) < 9 THEN uv.visitdate >= date_trunc(''week'', CURRENT_DATE) - interval ''1 week''
            WHEN ''Date'' = ''Month'' AND ''Off'' = ''Complete'' AND EXTRACT(HOUR FROM CURRENT_TIMESTAMP) < 9 THEN uv.visitdate >= date_trunc(''month'', CURRENT_DATE) - interval ''1 month'' 
            WHEN ''Date'' = ''Date'' AND ''Off'' = ''Partial'' AND EXTRACT(HOUR FROM CURRENT_TIMESTAMP) < 9 THEN (uv.visitdate + interval ''1'' day > CURRENT_DATE)
            WHEN ''Date'' = ''Week'' AND ''Off'' = ''Partial'' AND EXTRACT(HOUR FROM CURRENT_TIMESTAMP) < 9 THEN uv.visitdate >= date_trunc(''week'', CURRENT_DATE) - interval ''1 week'' 
            WHEN ''Date'' = ''Month'' AND ''Off'' = ''Partial'' AND EXTRACT(HOUR FROM CURRENT_TIMESTAMP) < 9 THEN uv.visitdate >= date_trunc(''month'', CURRENT_DATE) - interval ''1 month'' 
            ELSE FALSE
        END) THEN 1 ELSE 0 END) = 1
     AND REGEXP_LIKE(a.browser, ''Firefox$'')
GROUP BY
    1
HAVING COALESCE(COUNT(DISTINCT uv.sourceip), 0) > 0
ORDER BY
    2 DESC
FETCH NEXT 50 ROWS ONLY;' as query_text
from
(select distinct visitdate, any(countrycode) countrycode
  FROM uservisits 
  group by all
  limit 320);""",
    },
    "app_q15": {
        "query_start_ts": "2023-01-01 11:02:45",
        "query_end_ts": "2023-01-01 11:02:46",
        "generator_sql": """SELECT 'SELECT * FROM (
  SELECT *, DENSE_RANK() OVER (ORDER BY z___min_rank) as z___pivot_row_rank,
    RANK() OVER (PARTITION BY z__pivot_col_rank ORDER BY z___min_rank) as z__pivot_col_ordering,
    CASE WHEN z___min_rank = z___rank THEN 1 ELSE 0 END AS z__is_highest_ranked_cell
  FROM (
    SELECT *, MIN(z___rank) OVER (PARTITION BY t1dynamic_timeframe) as z___min_rank
    FROM (
      SELECT *, RANK() OVER (ORDER BY t1dynamic_timeframe ASC, z__pivot_col_rank) AS z___rank
      FROM (
        SELECT *, DENSE_RANK() OVER (ORDER BY CASE WHEN t1breakdown IS NULL THEN 1 ELSE 0 END, t1breakdown) AS z__pivot_col_rank
        FROM (
          SELECT
            CASE
              WHEN ''Date'' = ''Date'' THEN to_char(uv.visitdate, ''YYYY-MM-DD'')::VARCHAR
              WHEN ''Date'' = ''Week'' THEN to_char(date_trunc(''week'', uv.visitdate), ''YYYY-MM-DD'')::VARCHAR
              WHEN ''Date'' = ''Month'' THEN to_char(date_trunc(''month'', uv.visitdate), ''YYYY-MM'')::VARCHAR
            END AS t1dynamic_timeframe,
            s.word AS t1breakdown,
            COUNT(*) AS t1visits,
            SUM(CASE WHEN uv.duration > 30 THEN 1 ELSE 0 END) AS t1successful_visits
          FROM uservisits uv
          LEFT JOIN searchwords s ON uv.searchword = s.word
          WHERE uv.sourceip LIKE '''||sourceip_pattern||'%'' and visitdate between '''||visitdate||''' and '''||(visitdate+1)||''' 
          GROUP BY 1, 2
        ) ww
      ) bb WHERE z__pivot_col_rank <= 10000
    ) aa
  ) xx
) zz
WHERE (z__pivot_col_rank <= 50 OR z__is_highest_ranked_cell = 1) AND (z___pivot_row_rank <= 500 OR z__pivot_col_ordering = 1)
ORDER BY z___pivot_row_rank;' as query_text
from
(select distinct visitdate, any(substr(sourceip,1,3)) sourceip_pattern
  FROM uservisits 
  group by all
  limit 320);""",
   },
}

from firebolt.db import connect
from firebolt.client.auth import ClientCredentials
import os
import json
import random


def main():
    client_id = os.environ["FB_CLIENT_ID"]
    client_secret = os.environ["FB_CLIENT_SECRET"]
    account_name = os.environ["FB_ACCOUNT"]
    engine_name = os.environ["FB_ENGINE"]
    database = os.environ["FB_DATABASE"]
    api_endpoint = os.environ["FB_API"]

    connection = connect(
        auth=ClientCredentials(client_id, client_secret),
        account_name=account_name,
        engine_name=engine_name,
        database=database,
        api_endpoint=api_endpoint,
    )
    cursor = connection.cursor()
    scenarios = [{"concurrency":1, "sqls_list":[]}, {"concurrency":2, "sqls_list":[]},
                 {"concurrency":5, "sqls_list":[]}, {"concurrency":10, "sqls_list":[]},
                 {"concurrency":20, "sqls_list":[]}, {"concurrency":40, "sqls_list":[]}, 
                 {"concurrency":80, "sqls_list":[]}, {"concurrency":160, "sqls_list":[]},
                 {"concurrency":320, "sqls_list":[]}]
    for k, v in queries_templates.items():
        print(k)
        cursor.execute(v["generator_sql"])
        data = cursor.fetchall()
        i = 1        
        for row in data:
            for s in scenarios:
                if s["concurrency"] >= i:
                    s["sqls_list"].append({"sql_id": f"{k}_{i}","sql_text": row[0]})        
            i += 1
    for s in scenarios:
        sessions_list = []
        for session_num in range(1, s["concurrency"] + 1):
            # For each session, generate a unique sequence of queries
            session_queries = []
            random_queries = random.sample(list(queries_templates.keys()), len(queries_templates.keys()))
            for start_time, q in enumerate(random_queries, 1):
                session_queries.append({
                    "sql_id": [f"{q}_{session_num}"],
                    "start_time": start_time
                })
            sessions_list.append({
                "sess_id": f"session{session_num}",
                "sess_sqls": session_queries
            })
        output_json = {
            "sqls": s["sqls_list"],
            "sessions": sessions_list
        }        
        with open(f"firenewt_concurrency_{s["concurrency"]}.json", "w") as f1:
            json.dump(output_json, f1, indent=4)


if __name__ == "__main__":
    main()
