users = LOAD '/home/cloudera/Desktop/Pig/MovieDataSet/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int );

fltrd = FILTER users BY EqualsIgnoreCase(occupation, 'lawyer') AND EqualsIgnoreCase(gender, 'M');

grouped = GROUP fltrd ALL ;

summed = FOREACH grouped GENERATE group, COUNT(fltrd);

DUMP summed;

STORE summed INTO 'Q1Output';






