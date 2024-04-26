With origin_tab as (
    select
        *
    from
        uservisits
    where
  		visitdate between '2034-10-01'::PGDATE and '2034-12-31'::PGDATE
        and countrycode in ('ARG', 'SWE')
        and MATCH(destinationurl, '(ad|b$)') = true
        and adrevenue > 1.8075
),
searchwords_tab as (
    select * from searchwords where firstseen = '2034-02-17'::PGDATE
    union all
    select * from searchwords where firstseen = '2034-03-08'::PGDATE
    union all
    select * from searchwords where firstseen = '2038-07-13'::PGDATE
    union all
    select * from searchwords where firstseen between '2036-01-01'::PGDATE and '2036-12-31'::PGDATE
    union all
    select * from searchwords where firstseen between '2037-01-01'::PGDATE and '2037-06-30'::PGDATE
),
result_tab as (
    select *
    from origin_tab
    where visitdate between '2034-11-01'::PGDATE and '2034-12-31'::PGDATE
)
select 
(
    select
        ARRAY_AGG(destinationurl)
    from
        (
            select
                destinationurl
            from
                result_tab
            where
                searchword in (
                    select
                        distinct word
                    from
                        searchwords_tab
                    where
                        word_hash in (1412000111983818496)
                )
            order by
                adrevenue desc
            limit
                3
        )
) "f0"
,
(select ARRAY_AGG(destinationurl) from
(select destinationurl from result_tab
where
searchword in ( select distinct word from searchwords_tab 
where word_hash in (city_hash('u(g5>'))) 
order by adrevenue desc limit 3)) "f1"
,
(select ARRAY_AGG(destinationurl) from
(select destinationurl from result_tab
where
searchword in ( select distinct word from searchwords_tab 
where word_hash in (city_hash('u1gLZM8R{i'))) 
order by adrevenue desc limit 3)) "f2"
,
(select ARRAY_AGG(destinationurl) from
(select destinationurl from result_tab
where
searchword in ( select distinct word from searchwords_tab 
where word_hash in (city_hash('u1h5)Jb'))) 
order by adrevenue desc limit 3)) "f3"
,
(select ARRAY_AGG(destinationurl) from
(select destinationurl from result_tab
where
searchword in ( select distinct word from searchwords_tab 
where word_hash in (city_hash('u2MI'),city_hash('u)1220_@+A'),city_hash('u)1Dg'),city_hash('u)q\V:$0'),city_hash('u21+!5;'),city_hash('ts*'),city_hash('u25'),city_hash('vmD|:'),city_hash('xAz'),city_hash('|s#hD'))) 
order by adrevenue desc limit 3)) "f4"
,
(select ARRAY_AGG(destinationurl) from
(select destinationurl from result_tab
where
searchword in ( select distinct word from searchwords_tab 
where word_hash in (city_hash('u2?'))) 
order by adrevenue desc limit 3)) "f5"
,
(select ARRAY_AGG(destinationurl) from
(select destinationurl from result_tab
where
searchword in ( select distinct word from searchwords_tab 
where word_hash in (city_hash('u2@@'),city_hash('u)*'),city_hash('u(n+5'),city_hash('u)7OM2H.9T'),city_hash('u5F?x'),city_hash('|R/1'),city_hash('zt929|'))) 
order by adrevenue desc limit 3)) "f6"
,
(select ARRAY_AGG(destinationurl) from
(select destinationurl from result_tab
where
searchword in ( select distinct word from searchwords_tab 
where word_hash in (city_hash('u2X'))) 
order by adrevenue desc limit 3)) "f7";
