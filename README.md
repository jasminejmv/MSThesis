The code reads data from IMDB data files and creates key-value stores in Berkeley Db. 

Eg. actors.list file contains name of actor, and the list of movies and Episodes they acted in. For each line of source file, a actor-movie or actor-episode and actor-Tvseries key-value pair is generated and stored in Berkeley Db of the same name. Simultaneously a reverse pair is also generated of the form movie-actor, episode-actor and tvseries-actor and stored in the respective Berkeley Db. 
Similarly nine such files are processed to generate key-value stores.

The Berkeley Db key-value store facilitates retrieval of a record in constant time. Reverse key-value pair facilitates bidirectional queries.

Detailed description is available in the accompanying document.
