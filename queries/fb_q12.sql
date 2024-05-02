SELECT destinationurl
FROM   uservisits
WHERE  adrevenue between 1.8001
   and 1.80070239
GROUP BY destinationurl having count(*) > 100;
