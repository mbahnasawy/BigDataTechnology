REGISTER '/usr/lib/pig/piggybank.jar';

movies = LOAD '/home/cloudera/Desktop/Pig/MovieDataSet/movies.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (movieId:int, title:chararray, genres:chararray );

fltrd = FILTER movies BY STARTSWITH(title, 'A') OR STARTSWITH(title, 'a');

genresAll = FOREACH fltrd GENERATE genres;

flatThem = FOREACH genresAll GENERATE FLATTEN(TOKENIZE(genres, '|')) AS genFlat;

groupThem = GROUP flatThem BY genFlat ;

countThem = FOREACH groupThem GENERATE group, COUNT(flatThem);

sorted = ORDER countThem BY group;

DUMP sorted;

STORE sorted INTO 'Q3Output';



