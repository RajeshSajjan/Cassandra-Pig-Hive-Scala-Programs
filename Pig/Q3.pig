movies_data= load '/rxs130830/movies.dat' using PigStorage(':') as (MovieID:chararray, Title:chararray, Genres:chararray);
ratings_data= load '/rxs130830/ratings.dat' using PigStorage(':') as (UserID:chararray, MovieID:chararray, Rating:double, Timestamp:int);

movieRatings = cogroup movies_data by (MovieID), ratings_data by (MovieID);
joinResult = foreach movieRatings generate flatten(movies_data), flatten(ratings_data);
final = limit joinResult 6;
dump final;