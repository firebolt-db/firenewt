SELECT destinationurl,
       sum(adrevenue) as adrevenues
FROM   uservisits
WHERE  searchword = 'w1d>'
   and countrycode = 'CAN'
   and visitdate between '2035-09-08'
   and '2035-11-04'
   and regexp_like(destinationurl, '^COQP')
GROUP BY destinationurl
ORDER BY adrevenues desc, destinationurl limit 20000;
