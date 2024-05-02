with filtered_rankings as(SELECT *
                          FROM   rankings
                          WHERE  pagerank between 2000
                             and 3000)
SELECT allowed_records.pageurl,
       count(*) OVER () as total_rankings_count
FROM   filtered_rankings join (SELECT pageurl
                               FROM   filtered_rankings
                               ORDER BY avgduration desc limit 100) as allowed_records
        ON allowed_records.pageurl = filtered_rankings.pageurl
ORDER BY pagerank desc limit 20 offset 0;
