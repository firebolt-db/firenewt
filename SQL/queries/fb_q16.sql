with origin_tab as (SELECT *
                    FROM   uservisits
                    WHERE  visitdate between '2034-10-01'::pgdate
                       and '2034-12-31'::pgdate
                       and countrycode in ('ARG', 'SWE')
                       and match(destinationurl, '(ad|b$)') = true
                       and adrevenue > 1.8075), searchwords_tab as (SELECT *
                                             FROM   searchwords
                                             WHERE  firstseen = '2034-02-17'::pgdate
                                             UNION all
SELECT *
                                             FROM   searchwords
                                             WHERE  firstseen = '2034-03-08'::pgdate
                                             UNION all
SELECT *
                                             FROM   searchwords
                                             WHERE  firstseen = '2038-07-13'::pgdate
                                             UNION all
SELECT *
                                             FROM   searchwords
                                             WHERE  firstseen between '2036-01-01'::pgdate
                                                and '2036-12-31'::pgdate
                                             UNION all
SELECT *
                                             FROM   searchwords
                                             WHERE  firstseen between '2037-01-01'::pgdate
                                                and '2037-06-30'::pgdate), result_tab as (SELECT *
                                          FROM   origin_tab
                                          WHERE  visitdate between '2034-11-01'::pgdate
                                             and '2034-12-31'::pgdate)
SELECT (SELECT array_agg(destinationurl)
        FROM   (SELECT destinationurl
                FROM   result_tab
                WHERE  searchword in (SELECT DISTINCT word
                                      FROM   searchwords_tab
                                      WHERE  word_hash in (1412000111983818496))
                ORDER BY adrevenue desc limit 3)) "f0" , (SELECT array_agg(destinationurl)
                                          FROM   (SELECT destinationurl
                                                  FROM   result_tab
                                                  WHERE  searchword in (SELECT DISTINCT word
                                                                        FROM   searchwords_tab
                                                                        WHERE  word_hash in (city_hash('u(g5>')))
                                                                        ORDER BY adrevenue desc limit 3)) "f1" , (SELECT array_agg(destinationurl)
                                                                                    FROM   (SELECT destinationurl
                                                                                            FROM   result_tab
                                                                                            WHERE  searchword in (SELECT DISTINCT word
                                                                                                                  FROM   searchwords_tab
                                                                                                                  WHERE  word_hash in (city_hash('u1gLZM8R{i')))
                                                                                            ORDER BY adrevenue desc limit 3)) "f2" , (SELECT array_agg(destinationurl)
                                                                                    FROM   (SELECT destinationurl
                                                                                            FROM   result_tab
                                                                                            WHERE  searchword in (SELECT DISTINCT word
                                                                                                                  FROM   searchwords_tab
                                                                                                                  WHERE  word_hash in (city_hash('u1h5)Jb')))
                                                                                    ORDER BY adrevenue desc limit 3)) "f3" , (SELECT array_agg(destinationurl)
                                          FROM   (SELECT destinationurl
                                                  FROM   result_tab
                                                  WHERE  searchword in (SELECT DISTINCT word
                                                                        FROM   searchwords_tab
                                                                        WHERE  word_hash in (city_hash('u2MI'), city_hash('u)1220_@+A'), city_hash('u)1Dg'), city_hash('u)q\V:$0'), city_hash('u21+!5;'), city_hash('ts*'), city_hash('u25'), city_hash('vmD|:'), city_hash('xAz'), city_hash('|s#hD')))
ORDER BY adrevenue desc limit 3)) "f4" , (SELECT array_agg(destinationurl)
                                          FROM   (SELECT destinationurl
                                                  FROM   result_tab
                                                  WHERE  searchword in (SELECT DISTINCT word
                                                                        FROM   searchwords_tab
                                                                        WHERE  word_hash in (city_hash('u2?')))
                                                  ORDER BY adrevenue desc limit 3)) "f5" , (SELECT array_agg(destinationurl)
                                          FROM   (SELECT destinationurl
                                                  FROM   result_tab
                                                  WHERE  searchword in (SELECT DISTINCT word
                                                                        FROM   searchwords_tab
                                                                        WHERE  word_hash in (city_hash('u2@@'), city_hash('u)*'), city_hash('u(n+5'), city_hash('u)7OM2H.9T'), city_hash('u5F?x'), city_hash('|R/1'), city_hash('zt929|')))
                                          ORDER BY adrevenue desc limit 3)) "f6" , (SELECT array_agg(destinationurl)
                                          FROM   (SELECT destinationurl
                                                  FROM   result_tab
                                                  WHERE  searchword in (SELECT DISTINCT word
                                                                        FROM   searchwords_tab
                                                                        WHERE  word_hash in (city_hash('u2X')))
                                                  ORDER BY adrevenue desc limit 3)) "f7";
