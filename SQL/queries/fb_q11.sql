SELECT date_trunc('month', visitdate) as year_month_day,
       coalesce(sum(duration), 0) as "installs",
       coalesce(sum(length(searchword)), 0) as "billingCost",
       sum(case when adrevenue <= 1.5 then duration
                else 0 end) as "revenueD7"
FROM   uservisits
WHERE  (visitdate >= '2036-02-28'
   and visitdate <= date_add('DAY', 1, '2036-02-28'))
   and languagecode in ('DNK-DA', 'CHL-ES', 'NOR-NO', 'KOR-KO', 'POL-PL', 'AUS-EN', 'KWT-AR', 'PRI-ES')
GROUP BY 1
ORDER BY 1;
