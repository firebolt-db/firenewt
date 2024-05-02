SELECT searchword,
       useragent,
       languagecode
FROM   uservisits
WHERE  countrycode = 'AUS'
   and visitdate = '2036-03-30'
   and searchword in ('xVK(mv)LPl','xV!^xYL`','xV>x=','xVV17qN','xVXEmwqiP7','xV_q&UV','xVW)g5<M') limit 65;
