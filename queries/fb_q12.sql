select destinationurl from uservisits where adrevenue between 1.8001 and 1.80070239 group by destinationurl having count(*) > 100;
