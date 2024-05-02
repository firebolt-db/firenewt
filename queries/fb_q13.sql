with busiest_days as (SELECT visitdate,
                             count(*)
                      FROM   uservisits
                      GROUP BY 1
                      ORDER BY 2 desc limit 10)
SELECT countrycode,
       avg(length(searchword))
FROM   uservisits
WHERE  visitdate in (SELECT visitdate
                     FROM   busiest_days)
GROUP BY countrycode;
