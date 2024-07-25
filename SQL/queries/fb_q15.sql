SELECT countrycode,
       languagecode,
       count(distinct visitdate) as days_with_data,
       max(visitdate) last_visit,
       sum(adrevenue) sum_adrevenue,
       max(adrevenue) max_adrevenue,
       count(*) cnt
FROM   uservisits
WHERE  visitdate >= '2034-02-28'
   and visitdate <= '2038-04-28'
   and countrycode in ('PAN' ,'SLV' ,'NOR' ,'VNM' ,'NZL' ,'MKD' ,'GRC' ,'ITA' ,'IND' ,'JPN' ,'IDN' ,'PRI' ,'LBN' ,'ARG' ,'DNK' ,'IRL' ,'CHE' ,'TWN' ,'SAU' ,'ECU' ,'LVA' ,'AUS' ,'HRV' ,'SGP' ,'QAT' ,'FIN' ,'SYR' ,'GTM' ,'CAN' ,'BEL' ,'SRB')
   and languagecode in ('PRI-ES' ,'DNK-DA' ,'SYR-AR' ,'QAT-AR' ,'IRL-GA' ,'VNM-VI' ,'HRV-HR' ,'IDN-IN' ,'ARG-ES' ,'SAU-AR' ,'GRC-EL' ,'SLV-ES' ,'PAN-ES' ,'NZL-EN' ,'ITA-IT' ,'LBN-AR' ,'GTM-ES' ,'CHE-FR' ,'SRB-SR' ,'CAN-EN' ,'CHE-IT' ,'JPN-JA' ,'ECU-ES' ,'MKD-MK' ,'SGP-ZH' ,'NOR-NO' ,'AUS-EN' ,'FIN-FI' ,'TWN-ZH' ,'CHE-DE' ,'LVA-LV' ,'IND-HI' ,'BEL-FR')
   and useragent in ('Jxqjrolit/7.5' ,'Mrzif/6.5' ,'Buklieqonhjiwzqif/0.8')
GROUP BY 1, 2;
