movies_data= load '/rxs130830/movies.dat' using PigStorage(':') as (MovieID:chararray, Title:chararray, Genres:chararray);
ratings_data= load '/rxs130830/ratings.dat' using PigStorage(':') as (UserID:chararray, MovieID:chararray, Rating:double, Timestamp:int);

movie_Ratings = cogroup movies_data by (MovieID), ratings_data by (MovieID);
final = limit movie_Ratings 6;
dump final;