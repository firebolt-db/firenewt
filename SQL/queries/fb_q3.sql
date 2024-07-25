SELECT r.*,
       v.visitdate,
       v.adrevenue
FROM   uservisits v
    INNER JOIN rankings r
        ON v.destinationurl = r.pageurl
WHERE  sourceip = '129.66.135.24';
