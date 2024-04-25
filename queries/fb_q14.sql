SELECT countrycode, languagecode, count(length(searchword)) FROM uservisits where destinationurl in ( select pageurl from rankings where rankings.pagerank = 9899 ) group by 1,2;
