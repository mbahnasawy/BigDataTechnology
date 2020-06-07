REGISTER '/usr/lib/pig/piggybank.jar';

movies = LOAD '/home/cloudera/Desktop/Pig/MovieDataSet/movies.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (movieId:int, title:chararray, genres:chararray );

fltrMovies = FILTER movies BY (genres matches '.*Adventure.*');

ratings = LOAD '/home/cloudera/Desktop/Pig/MovieDataSet/rating.txt' AS (userId:int, movieId:int, rating:int, timestamp:int);

fltrRatines = FILTER ratings BY (rating == 5);

joined = JOIN fltrMovies BY movieId, fltrRatines BY movieId;

projected = FOREACH joined GENERATE $0 AS movieId, 'Adventure', $5 As rating ,$1 ;

distincted = DISTINCT projected;

sorted = ORDER distincted BY movieId;

top20 = LIMIT sorted 20;

DUMP top20;

STORE top20 INTO 'Q4Output';



