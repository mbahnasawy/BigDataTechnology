REGISTER '/usr/lib/pig/piggybank.jar';

movies = LOAD '/home/cloudera/Desktop/Pig/MovieDataSet/movies.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (movieId:int, title:chararray, genres:chararray );

fltrMovies = FILTER movies BY (genres matches '.*Adventure.*');

ratings = LOAD '/home/cloudera/Desktop/Pig/MovieDataSet/rating.txt' AS (userId:int, movieId:int, rating:int, timestamp:int);

fltrRatines = FILTER ratings BY (rating == 5);

joined = JOIN fltrMovies BY movieId, fltrRatines BY movieId;

projected = FOREACH joined GENERATE $0 AS MovieId, 'Adventure' AS Genre, $5 As Rating ,$1 AS Title;

distincted = DISTINCT projected;

sorted = ORDER distincted BY movieId;

top20 = LIMIT sorted 20;

STORE top20 INTO 'Q6Output' USING org.apache.pig.piggybank.storage.CSVExcelStorage('\t', 'YES_MULTILINE','UNIX', 'WRITE_OUTPUT_HEADER');




