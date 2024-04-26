SELECT date_trunc('month', visitdate) as year_month_day, 
       COALESCE(SUM(duration), 0) as "installs", 
       COALESCE(SUM(length(searchword)), 0) as "billingCost", 
       SUM(CASE WHEN adrevenue <= 1.5 THEN duration ELSE 0 END) as "revenueD7" 
FROM uservisits 
WHERE (visitdate >= '2036-02-28' AND visitdate <= DATE_ADD('DAY', 1, '2036-02-28')) AND 
        languagecode IN ( 'DNK-DA', 'CHL-ES', 'NOR-NO', 'KOR-KO', 'POL-PL', 'AUS-EN', 'KWT-AR', 'PRI-ES' ) 
GROUP BY 1 
ORDER BY 1;
