REGISTER /home/004/r/rx/rxs130830/pig_udf.jar;
movies_data= load '/rxs130830/movies.dat' using PigStorage(':') as (MovieID:chararray, Title:chararray, Genres:chararray);
final = FOREACH movies_data GENERATE Title, FormatGenre(Genres);
dump final;