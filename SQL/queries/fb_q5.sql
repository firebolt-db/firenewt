SELECT visitdate,
       sourceip,
       adrevenue
FROM   uservisits
WHERE  (visitdate between '2035-03-08'
   and '2035-04-04')
   and sourceip in ('126.98.46.113')
   and countrycode = 'IND';
