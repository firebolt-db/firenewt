SELECT languagecode,
       max(visitdate) as visitdate,
       array_join(array_agg(countrycode), ',') as countrycode
FROM   uservisits
WHERE  sourceip = '118.113.25.140'
GROUP BY languagecode;
