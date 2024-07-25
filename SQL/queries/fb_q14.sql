SELECT countrycode,
       languagecode,
       count(length(searchword))
FROM   uservisits
WHERE  destinationurl in (SELECT pageurl
                          FROM   rankings
                          WHERE  rankings.pagerank = 9899)
GROUP BY 1, 2;
