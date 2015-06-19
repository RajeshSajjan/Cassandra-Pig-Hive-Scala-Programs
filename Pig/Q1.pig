movies_data= load '/rxs130830/movies.dat' using PigStorage(':') as (MovieID:chararray, Title:chararray, Genres:chararray);
ratings_data= load '/rxs130830/ratings.dat' using PigStorage(':') as (UserID:chararray, MovieID:chararray, Rating:double, Timestamp:int);
users_data= load '/rxs130830/users.dat' using PigStorage(':') as (UserID:chararray, Gender:chararray, Age:int, Occupation:chararray, Zipcode:chararray);

movie_ratings = join movies_data by(MovieID), ratings_data by(MovieID);
comedy_drama = filter movie_ratings by (Genres matches '.*Comedy.*' and Genres matches '.*Drama.*'); 

groupA = group comedy_drama by $0;
groupB = foreach groupA generate flatten(group), AVG(comedy_drama.$5);
group_desc = order groupB by $1 desc;

limit_desc = limit group_desc 1;
A= foreach limit_desc generate $1;

jmovie= join groupB by ($1), A by ($0);
jratings = join ratings_data by(MovieID), jmovie by($0);
juserratings = join jratings by($0), users_data by($0);

Age = filter juserratings by (Gender matches '.*M.*' and (Age > 20 AND Age < 40) and Zipcode matches '1.*');
final_userID = foreach Age generate $0;
final = distinct final_userID;
dump final;