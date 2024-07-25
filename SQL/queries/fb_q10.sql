SELECT max(visitdate) as "latest_visit"
FROM   uservisits
WHERE  (visitdate >= '2036-02-28'
   and visitdate <= date_add('DAY', 1, '2036-02-28'));
