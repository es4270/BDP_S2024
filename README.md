ES4270 FINAL PROJECT

Hello :)

The goal of this project is to examine statistical effect of specific refs on specific team organizations (The Golden State Warriors, Los Angeles Lakers, etc), team rosters (The 2017 Warriors, the 2011 Suns), and players (Stephen Curry, Gary Payton II).

A bit about the data I used: Most of it was from this kaggle dataset, the most comprehensive NBA dataset I could find, 

https://www.kaggle.com/datasets/wyattowalsh/basketball

and this dataset for roster info.

https://www.kaggle.com/datasets/justinas/nba-players-data

The data only included up to the 2022-23 nba season, and was very spotty, and I had to remove many entries. Also, the player/roster info did not update before 1996, so I did 1996 and after. I used any regulation games, meaning regular season and playoffs, but not all star games, etc. I filtered out some statistically insignificant players.

1. The first thing I'd like you to do is open the spark client, with:
--spark-shell deploy-mode client

The data I used can be found in the "dataused" folder.

2. Next, I want to ingest the data, and get some barebones game, ref, player data. You'll just do this by running "teamandrefs.scala" which can be found in the "data_ingest" folder, using the ":load" command.

   Output will be found in four folders containing a single csv: 

   game_write.csv has all the game data w ref id's
   ref_write.csv has all the refs and their info
   player_write.csv has all the player's individual seasons and their season info
   gbr_write.csv has all their averages by season

3. Next, I want to process, clean, and sort the data so that I can look at player avg/total stats, as well as the corresponding stats to each referee. You will do this by running "ref_roster_analysis.scala" which can be found in the "etl_code" folder, using the ":load" command.

   Output will be found in 6 folders, each containing a single csv:
   player_stats has career avgs/totals for each player
   org_stats has career avgs/totals for each org
   ref_player has each player's roster's stats when their roster was reffed by a certain ref
   ref_org has each org's roster's stats when their team was reffed by a certain ref
   player_diff has the difference between each ref-player pair's stats difference from the player's regular stats
   org_diff has the difference between each ref-org pair's stats difference from the org's regular stats

4. Next, I want to have some basic dataprofiling functions, as well as a data profiling sample. You can activate these functions by running "dataprofile.scala", which can be found in the "profiling_code" folder, using the ":load" command

5. For more additional queries, I will be using the spark client to make queries in command line :)

Thank you!