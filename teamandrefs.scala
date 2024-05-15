//THE GOAL OF THIS IS TO READ IN ALL THE DATA FROM THE RELEVANT CSV'S, AND ORGANIZE
//THEM INTO 
//1. DATA OF EACH GAME W CORRESPONDING REFS/STATS,
//2. LIST OF ALL THE REFS WE'LL BE EXAMINING
//3. DATA OF ALL THE PLAYERZ WE'LL BE EXAMINING
//4. DATA OF EACH ROSTER, THEIR AVGS AND TOTALS OF HOME AND AWAY GAMES
// %% A "roster" I'm referring to is a specific iteration of a team, by season


//read in line_score.csv into df_ls, game_summary.csv into df_gs
val df_ls = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("line_score.csv")
val df_gs = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("game_summary.csv")

//drop unnecessary columns in line_score
var drop_ls_df = df_ls.drop(
	"game_sequence",
	"team_city_name_home","team_city_name_away","team_nickname_home",
	"team_nickname_away", "pts_ot1_home","pts_ot2_home","pts_ot3_home",
	"pts_ot4_home","pts_ot5_home","pts_ot6_home","pts_ot7_home","pts_ot8_home",
	"pts_ot9_home","pts_ot10_home","pts_ot1_away","pts_ot2_away","pts_ot3_away",
	"pts_ot4_away","pts_ot5_away","pts_ot6_away","pts_ot7_away","pts_ot8_away",
	"pts_ot9_away","pts_ot10_away", "team_wins_losses_home", "team_wins_losses_away"
)

//drop unnecessary columns in game_summary (every col except gameid and season)
var drop_gs_df = df_gs.drop(
	"game_date_est", "game_sequence", "game_status", "gamecode", "home_team_id",
	"visitor_team_id", "live_period", "live_pc_time", "natl_tv_broadcaster_abbreviation",
	"live_period_time_bcast", "wh_status", "game_status_text", "game_status_id"
).filter($"season" > 1995)

//IDK IF THIS COUNTS AS TEXT FORMATTING OR DATE FORMATTING BUT IT KINDA IS BOTH????
//extract just the year from the game_date data because that's all I need
var temp_yearsplit = drop_ls_df.withColumn("game_date_est", split($"game_date_est","-")(0))
//turn the year into an integer
var ls_year = temp_yearsplit.withColumn("game_date_est", col("game_date_est").cast("Integer"))
ls_year.count()

//filter out data after the year 1979 because it's irrelevant to me
//(advent of the 3pt line in the NBA)
var a = ls_year.filter($"game_date_est" > 1995)
// a.count()

//filter out invalid game id's
a = a.filter($"team_id_home" > 1610612700)
a = a.filter($"team_id_away" > 1610612700)

//filter out null gameid's
a = a.filter($"game_id".isNotNull())

// a.count()

//drop any nulls
a = a.na.drop()
var ls_df = a

// ls_df.groupBy("game_date_est").count().describe().collect().foreach(println)

//add seasons by joining game summary to valid id's
var seasonjoin = ls_df.join(drop_gs_df, "game_id")

//cut duplicate rows
var cleaning = seasonjoin.dropDuplicates()

//cut duplicate gameid's, should only be one game_id per game
cleaning = cleaning.dropDuplicates("game_id")
var cleaned = cleaning

//I HAVE HERE MADE A BINARY COLUMN BASED ON OTHER COLUMNS
//adding a result column, by comparing the home and away points of who won, 
//the home team? or the away team
var withresult1 = cleaned.withColumn("result",

	when($"pts_home" > $"pts_away", "H")
	.when($"pts_home" < $"pts_away", "A")
	.otherwise("")
)

//Add a column that attributes a value to win/loss for each team
var withresult = withresult1.withColumn("homeresult",
	when($"result" === "H", 1)
	.when($"result" === "A", 0)
).withColumn("awayresult",
	when($"result" === "A", 1)
	.when($"result" === "H", 0)
)

//other game data file, with the stats I felt were relevant
//thought these stats were relevant because of their
//prominence in twitter NBA discourse
val df_games = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("game.csv")
var drop_df_games = df_games.select(
	"game_id", "ftm_home", "fta_home", "pf_home", "ftm_away", "fta_away", "pf_away",
	"fg3_pct_home", "reb_home", "ast_home", "stl_home", "blk_home","tov_home",
	"fg3_pct_away", "reb_away", "ast_away", "stl_away", "blk_away","tov_away", 
)

//all ref data by game_id
val df_refs = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("officials.csv")
// df_refs.printSchema()
var drop_refs = df_refs.drop(
	"jersey_num"
)

//relevant game id's
var gameids = withresult.select("game_id", "season")

//attach the corresponding seasons to the gameids 
//that the ref data is attached to
var yearfind = drop_refs.join(gameids, Seq("game_id"),"inner")

//dropping the specialty teams (all star games, etc)
val teamdroplist = List("PHO", "WST", "EST", "DRT", "GNS","STP","LBN","AST", "UTH", "PHL", "SAN", "GOS")

//join the relevant reffing games with the relevant game data that has
//cleaned, making sure the teams aren't in that list above
var precomplete = withresult.join(drop_df_games, Seq("game_id")).filter(
	! $"team_abbreviation_home".isin(teamdroplist: _*)
).filter(! $"team_abbreviation_away".isin(teamdroplist: _*))

//stonks
val complete = precomplete
// complete.show(10)

//getting a list of every single ref per game, and condensing them to an array
//then making it a string so that i can write it to file later
var gamesrefs = yearfind.select(
	"game_id", "official_id"
).groupBy("game_id").agg(
	collect_list("official_id").name("official_id")
).withColumn(
	"official_id", concat_ws(",", $"official_id")
)

//all game data but with the refs as an array
var validgames = gamesrefs.join(complete, Seq("game_id"))

//basically make a "roster id", where it's a combo of the team abbrev
//and the year, and add it to each game id
var tempvalidgames_rosters = validgames.withColumn(
	"rosterhome", 
	concat_ws(
		"",col("team_abbreviation_home"),col("season"))).withColumn(
	"rosteraway", concat_ws("",col("team_abbreviation_away"),col("season")))

//getting the rosters of home teams by game id
var badhomegamez = tempvalidgames_rosters.select(
	"game_id","rosterhome"
).groupBy("rosterhome").count().withColumnRenamed(
	"rosterhome", "roster"
).withColumnRenamed(
	"count", "hcount"
)

//getting the rosters of away teams by game id
var badawaygamez = tempvalidgames_rosters.select(
	"game_id","rosteraway"
).groupBy("rosteraway").count().withColumnRenamed(
	"rosteraway", "roster"
).withColumnRenamed(
	"count", "acount"
)

//combining them so that there's a list of rosters by gameid, make sure
//there's enough games played total per roster
val goodrosterz = badhomegamez.join(badawaygamez, "roster").withColumn(
	"tcount", $"hcount" + $"acount"
).filter($"tcount" > 19)

//just a list of rosters that have played enough games,
//wanted to know how many rosters also
val rosterz = goodrosterz.select("roster").dropDuplicates()

//making sure that all the home teams are on the list of rosters we've kept
val tempvgrosters_home = tempvalidgames_rosters.join(
	rosterz, 
	tempvalidgames_rosters("rosterhome") === rosterz("roster"), 
	"left_semi"
)

//doing the same for away team rosters
val tempvgrosters_away = tempvalidgames_rosters.join(
	rosterz, 
	tempvalidgames_rosters("rosteraway") === rosterz("roster"), 
	"left_semi"
)

//combining so the data will only have valid home/away rosters
val validgames_rosters = tempvgrosters_home.join(tempvgrosters_away, Seq("game_id"), "left_semi")

//this is legit just the ref column from the old yearfind df
//so that i can have a list of refs
val ref_id_name = yearfind.drop("game_id", "season").dropDuplicates()

//getting a list of players
val df_playerz = spark.read.format("csv").option("header", "true").option("inferschema", "true").load("all_seasons.csv")

//dropping stuff idc about, and adding which roster they're a part of
//so that later I can match them to it
var drop_playerz = df_playerz.drop(
	"_c0","age","player_height","player_weight","college","draft_year","draft_round","draft_number",
	"oreb_pct","dreb_pct","ts_pct","ast_pct"
).na.drop().withColumn(
	"roster", concat_ws("", col("team_abbreviation"), substring(col("season"),0,4))
)

//get rid of players who aren't part of rosters I care about
var validplayers = drop_playerz.join(rosterz, drop_playerz("roster")===rosterz("roster"), "left_semi")

//calculating the averages of all home teams, as well as
//the total stats of all home teams
//as well as the record/result, and then adding a total count of home games
val groupbyhome = validgames_rosters.groupBy("rosterhome").agg(
	avg("pts_home").as("homepts_avg"),
	avg("pts_qtr1_home").as("homeptsq1_avg"),
	avg("pts_qtr2_home").as("homeptsq2_avg"),
	avg("pts_qtr3_home").as("homeptsq3_avg"),
	avg("pts_qtr4_home").as("homeptsq4_avg"),
	avg("homeresult").as("homeresult_avg"),
	avg("ftm_home").as("homeftm_avg"),
	avg("fta_home").as("homefta_avg"),
	avg("pf_home").as("homepf_avg"),
	avg("reb_home").as("homereb_avg"),
	avg("ast_home").as("homeast_avg"),
	avg("blk_home").as("homeblk_avg"),
	avg("tov_home").as("hometov_avg"),
	avg("pts_away").as("home_opppts_avg"),
	avg("ftm_away").as("home_oppftm_avg"),
	avg("fta_away").as("home_oppfta_avg"),
	avg("pf_away").as("home_opppf_avg"),
	avg("awayresult").as("home_oppresult_avg"),
	sum("pts_home").as("home_pts_total"),
	sum("homeresult").as("home_wins_total"),
	sum("ftm_home").as("home_ftm_total"),
	sum("fta_home").as("home_fta_total"),
	sum("pf_home").as("home_pf_total"),
	sum("reb_home").as("home_reb_total"),
	sum("ast_home").as("home_ast_total"),
	sum("blk_home").as("home_blk_total"),
	sum("tov_home").as("home_tov_total"),
	sum("pts_away").as("home_opppts_total"),
	sum("ftm_away").as("home_oppftm_total"),
	sum("fta_away").as("home_oppfta_total"),
	sum("pf_away").as("home_opppf_total"),
	sum("awayresult").as("home_oppresult_total")
).join(
	validgames_rosters.groupBy("rosterhome").count(), "rosterhome"
).withColumnRenamed(
	"rosterhome", "roster"
).withColumnRenamed(
	"count", "homecount"
)

//calculating the averages of all away teams, as well as
//the total stats of all away teams
//as well as the record/result, and then adding a total count of away games
val groupbyaway = validgames_rosters.groupBy("rosteraway").agg(
	avg("pts_away").as("awaypts_avg"),
	avg("pts_qtr1_away").as("awayptsq1_avg"),
	avg("pts_qtr2_away").as("awayptsq2_avg"),
	avg("pts_qtr3_away").as("awayptsq3_avg"),
	avg("pts_qtr4_away").as("awayptsq4_avg"),
	avg("awayresult").as("awayresult_avg"),
	avg("ftm_away").as("awayftm_avg"),
	avg("fta_away").as("awayfta_avg"),
	avg("pf_away").as("awaypf_avg"),
	avg("reb_away").as("awayreb_avg"),
	avg("ast_away").as("awayast_avg"),
	avg("blk_away").as("awayblk_avg"),
	avg("tov_away").as("awaytov_avg"),
	avg("pts_home").as("away_opppts_avg"),
	avg("ftm_home").as("away_oppftm_avg"),
	avg("fta_home").as("away_oppfta_avg"),
	avg("pf_home").as("away_opppf_avg"),
	avg("homeresult").as("away_oppresult_avg"),
	sum("pts_away").as("away_pts_total"),
	sum("awayresult").as("away_wins_total"),
	sum("ftm_away").as("away_ftm_total"),
	sum("fta_away").as("away_fta_total"),
	sum("pf_away").as("away_pf_total"),
	sum("reb_away").as("away_reb_total"),
	sum("ast_away").as("away_ast_total"),
	sum("blk_away").as("away_blk_total"),
	sum("tov_away").as("away_tov_total"),
	sum("pts_home").as("away_opppts_total"),
	sum("ftm_home").as("away_oppftm_total"),
	sum("fta_home").as("away_oppfta_total"),
	sum("pf_home").as("away_opppf_total"),
	sum("homeresult").as("away_oppresult_total")
).join(
	validgames_rosters.groupBy("rosteraway").count(), "rosteraway"
).withColumnRenamed(
	"rosteraway", "roster"
).withColumnRenamed(
	"count", "awaycount"
)

//joining the home team's avgs/totals and the away team's, and then adding
//a total count, as well as an organization(roster)
//this basically has home/away stats of rosters
val groupbyroster = groupbyhome.join(groupbyaway, "roster").withColumn(
	"org", substring($"roster",0,3)
).withColumn(
	"totalcount", $"homecount" + $"awaycount"
)

//writing the roster home/away stats to files
val gbr_write = groupbyroster.coalesce(1).write.format(
	"com.databricks.spark.csv").option("header", "true"
).save("gbr_write.csv")

//writing all the game data to a csv
val game_write = validgames_rosters.coalesce(1).write.format(
	"com.databricks.spark.csv").option("header", "true"
).save("game_write.csv")

//writing all the player data to a csv
val player_write = validplayers.coalesce(1).write.format(
	"com.databricks.spark.csv").option("header", "true"
).save("player_write.csv")

//writing all the ref data to a csv
val ref_id = ref_id_name.coalesce(1).write.format(
	"com.databricks.spark.csv").option("header", "true"
).save("ref_write.csv")

//i lowkey stopped here bc spark shell started crashing
//if i tried doing much more than this lol

























