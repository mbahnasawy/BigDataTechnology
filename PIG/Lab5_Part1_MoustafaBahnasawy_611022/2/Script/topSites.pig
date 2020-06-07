users = LOAD '/home/cloudera/Desktop/Pig/joinPig/users.csv' USING PigStorage(',') AS (name:chararray, age:int);
fltrd = FILTER users BY age >=18 AND age <=25;
pages = LOAD '/home/cloudera/Desktop/Pig/joinPig/pages.csv' USING PigStorage(',') AS (name:chararray, url:chararray);

joined = JOIN fltrd BY name, pages BY name;
grouped = GROUP joined BY url ;
groupedprojection = FOREACH grouped GENERATE group,joined.url As urls;
summed = FOREACH groupedprojection GENERATE group, COUNT(urls) AS clicks;

sorted = ORDER summed BY clicks DESC;
top5 = LIMIT sorted 5;
STORE top5 INTO 'top5SitesOutput';
DUMP top5;





