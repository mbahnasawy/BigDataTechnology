REGISTER '/usr/lib/pig/piggybank.jar';

movies = LOAD '/home/cloudera/Desktop/Pig/MovieDataSet/movies.csv' USING org.apache.pig.piggybank.storage.CSVLoader() AS (movieId:int, title:chararray, genres:chararray );

fltrMovies = FILTER movies BY (genres matches '.*Adventure.*');

ratings = LOAD '/home/cloudera/Desktop/Pig/MovieDataSet/rating.txt' AS (userId:int, movieId:int, rating:int, timestamp:int);

fltrRatines = FILTER ratings BY (rating == 5);

joined = JOIN fltrMovies BY movieId, fltrRatines BY movieId;

projected = FOREACH joined GENERATE $0 AS movieId,$1 AS title ;

distincted = DISTINCT projected;

sorted = ORDER distincted BY movieId;

top20 = LIMIT sorted 20;

users = LOAD '/home/cloudera/Desktop/Pig/MovieDataSet/users.txt' USING PigStorage('|') AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int );

fltrUsers = FILTER users BY EqualsIgnoreCase(occupation, 'programmer') AND EqualsIgnoreCase(gender, 'M');

fltrUsersRatings = JOIN fltrUsers BY userId, ratings BY userId;

fltrUsersRatingsProj = FOREACH fltrUsersRatings GENERATE $0 AS userId, $6 AS movieId;

joinUsersRatingsTop20 = JOIN fltrUsersRatingsProj BY movieId, top20 BY movieId;


groupedAll = GROUP joinUsersRatingsTop20 ALL ;
summedAll = FOREACH groupedAll GENERATE group, COUNT(joinUsersRatingsTop20 );

joinUsersRatingsTop20Proj = FOREACH joinUsersRatingsTop20 GENERATE title;

groupedByMovieTitle = GROUP joinUsersRatingsTop20Proj BY title ;
summedMovieTitle = FOREACH groupedByMovieTitle GENERATE group, COUNT(joinUsersRatingsTop20Proj);


DUMP summedAll;
DUMP summedMovieTitle;

STORE summedAll INTO 'Q5Output';
STORE summedMovieTitle INTO 'Q5OutputDetailed';


-- test = FILTER ratings BY movieId IN ( TOTUPLE(top20Proj));

-- ILLUSTRATE
