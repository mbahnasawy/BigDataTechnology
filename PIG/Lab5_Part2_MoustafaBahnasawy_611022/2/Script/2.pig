users = LOAD '/home/cloudera/Desktop/Pig/MovieDataSet/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int );

fltrd = FILTER users BY EqualsIgnoreCase(occupation, 'lawyer') AND EqualsIgnoreCase(gender, 'M');

sorted = ORDER fltrd BY age DESC;
oldest = LIMIT sorted 1 ;
hisId = FOREACH oldest GENERATE 'Oldest Lawyer Id' , userId;

DUMP hisId;

STORE hisId INTO 'Q2Output';
