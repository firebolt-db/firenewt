with desktop as (SELECT date_trunc('month', visitdate) as year_month_day,
                        sourceip,
                        countrycode,
                        count(*) as visits,
                        sum(adrevenue) as adrevenue,
                        count(distinct languagecode) as languagecode,
                        max(length(searchword)) as searchwordlength,
                        sum(duration) as time_on_site
                 FROM   uservisits
                 WHERE  sourceip in ('129.80.11.38')
                    and countrycode < 'zzz'
                    and visitdate between '2038-02-28'
                    and '2039-07-29'
                    and useragent = 'Rpwuh/1.2'
                 GROUP BY sourceip, countrycode, year_month_day), mobile as (SELECT date_trunc('month', visitdate) as year_month_day,
                                                                   sourceip,
                                                                   countrycode,
                                                                   count(*) as visits,
                                                                   sum(adrevenue) as adrevenue,
                                                                   count(distinct languagecode) as languagecode,
                                                                   max(length(searchword)) as searchwordlength,
                                                                   sum(duration) as time_on_site
                                                            FROM   uservisits
                                                            WHERE  sourceip in ('129.80.11.38')
                                                               and countrycode < 'zzz'
                                                               and visitdate between '2038-02-28'
                                                               and '2039-07-29'
                                                               and useragent = 'Krjxhzmhpzwlaau/2.6'
                                                            GROUP BY sourceip, countrycode, year_month_day)
SELECT coalesce(desktop.year_month_day, mobile.year_month_day) as year_month_day,
       coalesce(desktop.sourceip, mobile.sourceip) as site,
       coalesce(desktop.countrycode, mobile.countrycode) as countrycode,
       coalesce(desktop.visits, 0) + coalesce(mobile.visits, 0) as visits,
       coalesce(desktop.adrevenue, 0)+ coalesce(mobile.adrevenue, 0) as page_views,
       coalesce(desktop.searchwordlength,
                                                        0) + coalesce(mobile.searchwordlength, 0) as searchwordlength,
       coalesce(desktop.time_on_site,
                                                    0) + coalesce(mobile.time_on_site, 0) as time_on_site
FROM   desktop full
    OUTER JOIN mobile
        ON mobile.year_month_day = desktop.year_month_day and
           mobile.sourceip = desktop.sourceip and
           mobile.countrycode = desktop.countrycode;
