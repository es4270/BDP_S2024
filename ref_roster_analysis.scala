//I'M SORRY THIS SO LONG AND UGLY
//GOALS
//FROM ALL THE DATA OUTPUT BY TEAMANDREFS.SCALA:
//1. BE ABLE TO PULL IN DATA OF HOME/AWAY GAMES AND MAKE TOTAL STATS
//2. BE ABLE TO PULL IN DATA OF EACH PLAYER FOR *ONLY THE GAMES I'M LOOKING AT*
//3. PULL IN THE REFS AND THE CORRESPONDING STATS/AVGS OF A TEAM FOR 
//   WHEN THAT SPECIFIC REF IS REFFING THAT TEAM
//4. PULL IN REFS AND THE CORRESPONDING STATS/AVGS OF A PLAYER FOR WHEN
//   THAT SPECIFIC REF IS REFFING THAT PLAYER

//basically this is reading the home/away avgs/totals data I read earlier by roster
//and then making total averages/totals, ie home and away combined
val df_gbr = spark.read.format(
	"csv"
).option(
	"header", "true"
).option(
	"inferschema", "true"
).load("hdfs://nyu-dataproc-m/user/es4270_nyu_edu/gbr_write.csv"
).withColumn(
	"org", substring($"roster",0,3)
).withColumn(
	"totalcount", $"homecount" + $"awaycount"
).withColumn(
	"pts_avg", 
	(($"awaypts_avg" * $"awaycount") + ($"homepts_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"result_avg", 
	(($"awayresult_avg" * $"awaycount") + ($"homeresult_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"fta_avg", 
	(($"awayfta_avg" * $"awaycount") + ($"homefta_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"ftm_avg", 
	(($"awayftm_avg" * $"awaycount") + ($"homeftm_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"pf_avg", 
	(($"awaypf_avg" * $"awaycount") + ($"homepf_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"reb_avg", 
	(($"awayreb_avg" * $"awaycount") + ($"homereb_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"ast_avg", 
	(($"awayast_avg" * $"awaycount") + ($"homeast_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"blk_avg", 
	(($"awayblk_avg" * $"awaycount") + ($"homeblk_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"tov_avg", 
	(($"awaytov_avg" * $"awaycount") + ($"hometov_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"opp_pts_avg", 
	(($"away_opppts_avg" * $"awaycount") + ($"home_opppts_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"opp_ftm_avg", 
	(($"away_oppftm_avg" * $"awaycount") + ($"home_oppftm_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"opp_fta_avg", 
	(($"away_oppfta_avg" * $"awaycount") + ($"home_oppfta_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"opp_pf_avg", 
	(($"away_opppf_avg" * $"awaycount") + ($"home_opppf_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"opp_result_avg", 
	(($"away_oppresult_avg" * $"awaycount") + ($"home_oppresult_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"total_pts",
	$"home_pts_total" + $"away_pts_total"
).withColumn(
	"total_wins",
	$"home_wins_total" + $"away_wins_total"
).withColumn(
	"total_ftm",
	$"home_ftm_total" + $"away_ftm_total"
).withColumn(
	"total_fta",
	$"home_fta_total" + $"away_fta_total"
).withColumn(
	"total_pf",
	$"home_pf_total" + $"away_pf_total"
).withColumn(
	"total_reb",
	$"home_reb_total" + $"away_reb_total"
).withColumn(
	"total_ast",
	$"home_ast_total" + $"away_ast_total"
).withColumn(
	"total_blk",
	$"home_blk_total" + $"away_blk_total"
).withColumn(
	"total_tov",
	$"home_tov_total" + $"away_tov_total"
).withColumn(
	"total_opppts",
	$"home_opppts_total" + $"away_opppts_total"
).withColumn(
	"total_oppftm",
	$"home_oppftm_total" + $"away_oppftm_total"
).withColumn(
	"total_oppfta",
	$"home_oppfta_total" + $"away_oppfta_total"
).withColumn(
	"total_opppf",
	$"home_opppf_total" + $"away_opppf_total"
).withColumn(
	"total_losses",
	$"home_oppresult_total" + $"away_oppresult_total"
)

//reading in the data of the games
val df_games = spark.read.format(
	"csv"
).option(
	"header", "true"
).option(
	"inferschema", "true"
).load("hdfs://nyu-dataproc-m/user/es4270_nyu_edu/game_write.csv")

//reading in the ref data, and combining the first/last name into one name
val df_ref = spark.read.format(
	"csv"
).option(
	"header", "true"
).option(
	"inferschema", "true"
).load("hdfs://nyu-dataproc-m/user/es4270_nyu_edu/ref_write.csv"
).withColumn(
	"official_name", concat_ws(" ", $"first_name", $"last_name")
).drop("first_name","last_name")

//reading in the player data
val df_players = spark.read.format(
	"csv"
).option(
	"header", "true"
).option(
	"inferschema", "true"
).load("hdfs://nyu-dataproc-m/user/es4270_nyu_edu/player_write.csv")

//reading in all the game data, and then making it so that in games that have
//multiple referees (all of them), for each ref,
//there is a row of game data, the teams who played, etc
val games = df_games.withColumn(
	"officials", split($"official_id", ",")
).withColumn(
	"official_id", explode($"officials")
).withColumn(
	"official_id", $"official_id".cast("Integer")
).join(df_ref.select("official_id","official_name"), Seq("official_id"))

//compiling all the avgs, totals, for how a roster performs when being
//reffed by one specific ref in a home game
val gb_refroster_home = games.groupBy("official_name", "rosterhome").agg(
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
	games.groupBy(
		"official_name","rosterhome").count(), Seq("official_name", "rosterhome")
).withColumnRenamed(
	"rosterhome", "roster"
).withColumnRenamed(
	"count", "homecount"
)

//compiling all the avgs, totals, for how a roster performs when being
//reffed by one specific ref in a away game
val gb_refroster_away = games.groupBy("official_name", "rosteraway").agg(
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
	games.groupBy("official_name","rosteraway").count(), Seq("official_name", "rosteraway")
).withColumnRenamed(
	"rosteraway", "roster"
).withColumnRenamed(
	"count", "awaycount"
)

//combining home and away data so that we can see
//how the roster did as a whole, between home and away,
//while being reffed by that referee
val gb_refroster_total = gb_refroster_home.join(gb_refroster_away, Seq("official_name", "roster")
).withColumn(
	"org", substring($"roster",0,3)
).withColumn(
	"totalcount", $"homecount" + $"awaycount"
).withColumn(
	"pts_avg", 
	(($"awaypts_avg" * $"awaycount") + ($"homepts_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"result_avg", 
	(($"awayresult_avg" * $"awaycount") + ($"homeresult_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"fta_avg", 
	(($"awayfta_avg" * $"awaycount") + ($"homefta_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"ftm_avg", 
	(($"awayftm_avg" * $"awaycount") + ($"homeftm_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"pf_avg", 
	(($"awaypf_avg" * $"awaycount") + ($"homepf_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"reb_avg", 
	(($"awayreb_avg" * $"awaycount") + ($"homereb_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"ast_avg", 
	(($"awayast_avg" * $"awaycount") + ($"homeast_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"blk_avg", 
	(($"awayblk_avg" * $"awaycount") + ($"homeblk_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"tov_avg", 
	(($"awaytov_avg" * $"awaycount") + ($"hometov_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"opp_pts_avg", 
	(($"away_opppts_avg" * $"awaycount") + ($"home_opppts_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"opp_ftm_avg", 
	(($"away_oppftm_avg" * $"awaycount") + ($"home_oppftm_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"opp_fta_avg", 
	(($"away_oppfta_avg" * $"awaycount") + ($"home_oppfta_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"opp_pf_avg", 
	(($"away_opppf_avg" * $"awaycount") + ($"home_opppf_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"opp_result_avg", 
	(($"away_oppresult_avg" * $"awaycount") + ($"home_oppresult_avg" * $"homecount"))/ $"totalcount"
).withColumn(
	"total_pts",
	$"home_pts_total" + $"away_pts_total"
).withColumn(
	"total_wins",
	$"home_wins_total" + $"away_wins_total"
).withColumn(
	"total_ftm",
	$"home_ftm_total" + $"away_ftm_total"
).withColumn(
	"total_fta",
	$"home_fta_total" + $"away_fta_total"
).withColumn(
	"total_pf",
	$"home_pf_total" + $"away_pf_total"
).withColumn(
	"total_reb",
	$"home_reb_total" + $"away_reb_total"
).withColumn(
	"total_ast",
	$"home_ast_total" + $"away_ast_total"
).withColumn(
	"total_blk",
	$"home_blk_total" + $"away_blk_total"
).withColumn(
	"total_tov",
	$"home_tov_total" + $"away_tov_total"
).withColumn(
	"total_opppts",
	$"home_opppts_total" + $"away_opppts_total"
).withColumn(
	"total_oppftm",
	$"home_oppftm_total" + $"away_oppftm_total"
).withColumn(
	"total_oppfta",
	$"home_oppfta_total" + $"away_oppfta_total"
).withColumn(
	"total_opppf",
	$"home_opppf_total" + $"away_opppf_total"
).withColumn(
	"total_losses",
	$"home_oppresult_total" + $"away_oppresult_total"
)

//filtering out players who haven't played 60 games in their
//entire career
//i picked 60 bc you need 60 games for all season awards,
//also it's a neat number > 2/3 of an nba season
//in theory there are 29 other teams in the league at one time
//so you can play every other team in 58 games in theory
//i will however admit that this is pretty arbitrary
var playerz_w_enuf_gamez = df_players.groupBy(
	"player_name"
).sum("gp").filter(
	$"sum(gp)" > 60)

//making sure our player data reflects this list of players, and just taking
//the player name and roster, tbh I can't use the rest of the data
val valid_playerz = df_players.join(
	playerz_w_enuf_gamez, Seq("player_name"), "left_semi"
).select("player_name","roster")

//putting all the players who were in a roster at one time into an array
val rosterz_w_players = valid_playerz.groupBy("roster").agg(
	collect_list("player_name").name("players")
)

//making it so that all the refs and rosters they have reffed's performances
//have the full player lists attached,
//then exploding it so that for each player, there is a row for
//how each roster they were on performed for
//each specific ref
val complete_refs = gb_refroster_total.join(
	rosterz_w_players, Seq("roster")
).withColumn(
	"player_name", explode($"players")
).drop("players")

//just their averages, and totals, but only with corresponding rosters, refs
//don't have corresponding rows
val complete_norefs = df_gbr.join(
	rosterz_w_players, Seq("roster")
).withColumn(
	"player_name", explode($"players")
).drop("players")

//totals and avgs of a player over their entire career
val player_stats = complete_norefs.groupBy(
	"player_name"
).agg(
	sum("totalcount").as("career_team_gp"),
	sum("total_pts").as("career_team_pts"),
	sum("total_wins").as("career_team_wins"),
	sum("total_ftm").as("career_team_ftm"),
	sum("total_fta").as("career_team_fta"),
	sum("total_pf").as("career_team_pf"),
	sum("total_reb").as("career_team_reb"),
	sum("total_ast").as("career_team_blk"),
	sum("total_tov").as("career_team_tov"),
	sum("total_opppts").as("career_team_opppts"),
	sum("total_oppftm").as("career_team_oppftm"),
	sum("total_oppfta").as("career_team_oppfta"),
	sum("total_opppf").as("career_team_opppf"),
	sum("total_losses").as("career_team_losses")
).withColumn(
	"career_team_avg_pts",
	$"career_team_pts"/ $"career_team_gp"
).withColumn(
	"career_team_avg_wins",
	$"career_team_wins"/ $"career_team_gp"
).withColumn(
	"career_team_avg_ftm",
	$"career_team_ftm"/ $"career_team_gp"
).withColumn(
	"career_team_avg_fta",
	$"career_team_fta"/ $"career_team_gp"
).withColumn(
	"career_team_avg_pf",
	$"career_team_pf"/ $"career_team_gp"
).withColumn(
	"career_team_avg_reb",
	$"career_team_ftm"/ $"career_team_gp"
).withColumn(
	"career_team_avg_ast",
	$"career_team_pts"/ $"career_team_gp"
).withColumn(
	"career_team_avg_tov",
	$"career_team_wins"/ $"career_team_gp"
).withColumn(
	"career_team_avg_opppts",
	$"career_team_ftm"/ $"career_team_gp"
).withColumn(
	"career_team_avg_oppftm",
	$"career_team_pts"/ $"career_team_gp"
).withColumn(
	"career_team_avg_oppfta",
	$"career_team_wins"/ $"career_team_gp"
).withColumn(
	"career_team_avg_opppf",
	$"career_team_ftm"/ $"career_team_gp"
).withColumn(
	"WLpct",
	$"career_team_wins"/ $"career_team_gp"
).withColumn(
	"WinLoss",
	concat_ws(" - ", $"career_team_wins", $"career_team_losses")
)


//player career totals/avgs, but by how they performed while being
//reffed by a specific ref
val ref_player_comps = complete_refs.groupBy(
	"official_name", "player_name"
).agg(
	sum("totalcount").as("career_team_gp"),
	sum("total_pts").as("career_team_pts"),
	sum("total_wins").as("career_team_wins"),
	sum("total_ftm").as("career_team_ftm"),
	sum("total_fta").as("career_team_fta"),
	sum("total_pf").as("career_team_pf"),
	sum("total_reb").as("career_team_reb"),
	sum("total_ast").as("career_team_blk"),
	sum("total_tov").as("career_team_tov"),
	sum("total_opppts").as("career_team_opppts"),
	sum("total_oppftm").as("career_team_oppftm"),
	sum("total_oppfta").as("career_team_oppfta"),
	sum("total_opppf").as("career_team_opppf"),
	sum("total_losses").as("career_team_losses")
).filter($"career_team_gp" > 5).withColumn(
	"career_team_avg_pts",
	$"career_team_pts"/ $"career_team_gp"
).withColumn(
	"career_team_avg_wins",
	$"career_team_wins"/ $"career_team_gp"
).withColumn(
	"career_team_avg_ftm",
	$"career_team_ftm"/ $"career_team_gp"
).withColumn(
	"career_team_avg_fta",
	$"career_team_fta"/ $"career_team_gp"
).withColumn(
	"career_team_avg_pf",
	$"career_team_pf"/ $"career_team_gp"
).withColumn(
	"career_team_avg_reb",
	$"career_team_ftm"/ $"career_team_gp"
).withColumn(
	"career_team_avg_ast",
	$"career_team_pts"/ $"career_team_gp"
).withColumn(
	"career_team_avg_tov",
	$"career_team_wins"/ $"career_team_gp"
).withColumn(
	"career_team_avg_opppts",
	$"career_team_ftm"/ $"career_team_gp"
).withColumn(
	"career_team_avg_oppftm",
	$"career_team_pts"/ $"career_team_gp"
).withColumn(
	"career_team_avg_oppfta",
	$"career_team_wins"/ $"career_team_gp"
).withColumn(
	"career_team_avg_opppf",
	$"career_team_ftm"/ $"career_team_gp"
).withColumn(
	"WLpct",
	$"career_team_wins"/ $"career_team_gp"
).withColumn(
	"WinLoss",
	concat_ws(" - ", $"career_team_wins", $"career_team_losses")
)

//stats of an organization at default in the games that I can cover
val org_stats = df_gbr.groupBy(
	"org"
).agg(
	sum("totalcount").as("career_team_gp"),
	sum("total_pts").as("career_team_pts"),
	sum("total_wins").as("career_team_wins"),
	sum("total_ftm").as("career_team_ftm"),
	sum("total_fta").as("career_team_fta"),
	sum("total_pf").as("career_team_pf"),
	sum("total_reb").as("career_team_reb"),
	sum("total_ast").as("career_team_blk"),
	sum("total_tov").as("career_team_tov"),
	sum("total_opppts").as("career_team_opppts"),
	sum("total_oppftm").as("career_team_oppftm"),
	sum("total_oppfta").as("career_team_oppfta"),
	sum("total_opppf").as("career_team_opppf"),
	sum("total_losses").as("career_team_losses")
).filter($"career_team_gp" > 10).withColumn(
	"career_team_avg_pts",
	$"career_team_pts"/ $"career_team_gp"
).withColumn(
	"career_team_avg_wins",
	$"career_team_wins"/ $"career_team_gp"
).withColumn(
	"career_team_avg_ftm",
	$"career_team_ftm"/ $"career_team_gp"
).withColumn(
	"career_team_avg_fta",
	$"career_team_fta"/ $"career_team_gp"
).withColumn(
	"career_team_avg_pf",
	$"career_team_pf"/ $"career_team_gp"
).withColumn(
	"career_team_avg_reb",
	$"career_team_ftm"/ $"career_team_gp"
).withColumn(
	"career_team_avg_ast",
	$"career_team_pts"/ $"career_team_gp"
).withColumn(
	"career_team_avg_tov",
	$"career_team_wins"/ $"career_team_gp"
).withColumn(
	"career_team_avg_opppts",
	$"career_team_ftm"/ $"career_team_gp"
).withColumn(
	"career_team_avg_oppftm",
	$"career_team_pts"/ $"career_team_gp"
).withColumn(
	"career_team_avg_oppfta",
	$"career_team_wins"/ $"career_team_gp"
).withColumn(
	"career_team_avg_opppf",
	$"career_team_ftm"/ $"career_team_gp"
).withColumn(
	"WLpct",
	$"career_team_wins"/ $"career_team_gp"
).withColumn(
	"WinLoss",
	concat_ws(" - ", $"career_team_wins", $"career_team_losses")
)


//how each organization does in their avgs/totals when being reffed by a
//specific ref
val ref_org_comps = gb_refroster_total.groupBy(
	"official_name", "org"
).agg(
	sum("totalcount").as("career_team_gp"),
	sum("total_pts").as("career_team_pts"),
	sum("total_wins").as("career_team_wins"),
	sum("total_ftm").as("career_team_ftm"),
	sum("total_fta").as("career_team_fta"),
	sum("total_pf").as("career_team_pf"),
	sum("total_reb").as("career_team_reb"),
	sum("total_ast").as("career_team_blk"),
	sum("total_tov").as("career_team_tov"),
	sum("total_opppts").as("career_team_opppts"),
	sum("total_oppftm").as("career_team_oppftm"),
	sum("total_oppfta").as("career_team_oppfta"),
	sum("total_opppf").as("career_team_opppf"),
	sum("total_losses").as("career_team_losses")
).filter($"career_team_gp" > 10).withColumn(
	"career_team_avg_pts",
	$"career_team_pts"/ $"career_team_gp"
).withColumn(
	"career_team_avg_wins",
	$"career_team_wins"/ $"career_team_gp"
).withColumn(
	"career_team_avg_ftm",
	$"career_team_ftm"/ $"career_team_gp"
).withColumn(
	"career_team_avg_fta",
	$"career_team_fta"/ $"career_team_gp"
).withColumn(
	"career_team_avg_pf",
	$"career_team_pf"/ $"career_team_gp"
).withColumn(
	"career_team_avg_reb",
	$"career_team_ftm"/ $"career_team_gp"
).withColumn(
	"career_team_avg_ast",
	$"career_team_pts"/ $"career_team_gp"
).withColumn(
	"career_team_avg_tov",
	$"career_team_wins"/ $"career_team_gp"
).withColumn(
	"career_team_avg_opppts",
	$"career_team_ftm"/ $"career_team_gp"
).withColumn(
	"career_team_avg_oppftm",
	$"career_team_pts"/ $"career_team_gp"
).withColumn(
	"career_team_avg_oppfta",
	$"career_team_wins"/ $"career_team_gp"
).withColumn(
	"career_team_avg_opppf",
	$"career_team_ftm"/ $"career_team_gp"
).withColumn(
	"WLpct",
	$"career_team_wins"/ $"career_team_gp"
).withColumn(
	"WinLoss",
	concat_ws(" - ", $"career_team_wins", $"career_team_losses")
)


//what stat avgs i want to see difference in between player's avg and
//when they play w ref
var diff_stats = player_stats.select(
	"player_name", "career_team_avg_pts", "career_team_avg_ftm", "career_team_avg_fta", 
	"career_team_avg_pf", "career_team_avg_reb", "career_team_avg_ast", "career_team_avg_tov", 
	"career_team_avg_opppts", "career_team_avg_oppftm", "career_team_avg_oppfta", "career_team_avg_opppf",
	"WLpct", "WinLoss"
).withColumnRenamed(
	"career_team_avg_pts", "cta_pts"
).withColumnRenamed(
	"career_team_avg_ftm", "cta_ftm"
).withColumnRenamed(
	"career_team_avg_fta", "cta_fta"
).withColumnRenamed(
	"career_team_avg_pf", "cta_pf"
).withColumnRenamed(
	"career_team_avg_reb", "cta_reb"
).withColumnRenamed(
	"career_team_avg_ast", "cta_ast"
).withColumnRenamed(
	"career_team_avg_tov", "cta_tov"
).withColumnRenamed(
	"career_team_avg_opppts", "cta_opppts"
).withColumnRenamed(
	"career_team_avg_oppftm", "cta_oppftm"
).withColumnRenamed(
	"career_team_avg_oppfta", "cta_oppfta"
).withColumnRenamed(
	"career_team_avg_opppf", "cta_opppf"
).withColumnRenamed(
	"WLpct", "cta_WLpct"
).withColumnRenamed(
	"WinLoss", "cta_WinLoss"
)

//player's diff w each ref
val player_diff = ref_player_comps.select(
	"player_name", "official_name", "career_team_avg_pts", "career_team_avg_ftm", "career_team_avg_fta",
	"career_team_avg_pf", "career_team_avg_reb", "career_team_avg_ast", "career_team_avg_tov",
	"career_team_avg_opppts", "career_team_avg_oppftm", "career_team_avg_oppfta", "career_team_avg_opppf",
	"WLpct", "WinLoss", "career_team_gp"
).join(diff_stats, Seq("player_name")).withColumn(
	"career_team_avg_pts", $"career_team_avg_pts" - $"cta_pts"
).withColumn(
	"career_team_avg_ftm", $"career_team_avg_ftm" - $"cta_ftm"
).withColumn(
	"career_team_avg_fta", $"career_team_avg_fta" - $"cta_fta"
).withColumn(
	"career_team_avg_pf", $"career_team_avg_pf" - $"cta_pf"
).withColumn(
	"career_team_avg_reb", $"career_team_avg_reb" - $"cta_reb"
).withColumn(
	"career_team_avg_ast", $"career_team_avg_ast" - $"cta_ast"
).withColumn(
	"career_team_avg_tov", $"career_team_avg_tov" - $"cta_tov"
).withColumn(
	"career_team_avg_opppts", $"career_team_avg_pts" - $"cta_pts"
).withColumn(
	"career_team_avg_oppftm", $"career_team_avg_oppftm" - $"cta_oppftm"
).withColumn(
	"career_team_avg_oppfta", $"career_team_avg_oppfta" - $"cta_oppfta"
).withColumn(
	"career_team_avg_opppf", $"career_team_avg_opppf" - $"cta_opppf"
).withColumn(
	"WLpct", $"WLpct" - $"cta_WLpct"
).withColumn(
	"WinLoss", concat_ws(" ||| ",$"WinLoss", $"cta_WinLoss")
).drop(
	"cta_pts", "cta_ftm", "cta_fta", "cta_pf", "cta_reb", "cta_ast", "cta_tov", "cta_opppts",
	"cta_oppftm", "cta_oppfta", "cta_opppf", "cta_WLpct", "cta_WinLoss"
)

//stats that i want to compare org percentages on w refs
var org_diff_stats = org_stats.select(
	"org", "career_team_avg_pts", "career_team_avg_ftm", "career_team_avg_fta", 
	"career_team_avg_pf", "career_team_avg_reb", "career_team_avg_ast", "career_team_avg_tov", 
	"career_team_avg_opppts", "career_team_avg_oppftm", "career_team_avg_oppfta", "career_team_avg_opppf",
	"WLpct", "WinLoss"
).withColumnRenamed(
	"career_team_avg_pts", "cta_pts"
).withColumnRenamed(
	"career_team_avg_ftm", "cta_ftm"
).withColumnRenamed(
	"career_team_avg_fta", "cta_fta"
).withColumnRenamed(
	"career_team_avg_pf", "cta_pf"
).withColumnRenamed(
	"career_team_avg_reb", "cta_reb"
).withColumnRenamed(
	"career_team_avg_ast", "cta_ast"
).withColumnRenamed(
	"career_team_avg_tov", "cta_tov"
).withColumnRenamed(
	"career_team_avg_opppts", "cta_opppts"
).withColumnRenamed(
	"career_team_avg_oppftm", "cta_oppftm"
).withColumnRenamed(
	"career_team_avg_oppfta", "cta_oppfta"
).withColumnRenamed(
	"career_team_avg_opppf", "cta_opppf"
).withColumnRenamed(
	"WLpct", "cta_WLpct"
).withColumnRenamed(
	"WinLoss", "cta_WinLoss"
)

//same thing as above but with orgs
val org_diff = ref_org_comps.select(
	"org", "official_name", "career_team_avg_pts", "career_team_avg_ftm", "career_team_avg_fta",
	"career_team_avg_pf", "career_team_avg_reb", "career_team_avg_ast", "career_team_avg_tov",
	"career_team_avg_opppts", "career_team_avg_oppftm", "career_team_avg_oppfta", "career_team_avg_opppf",
	"WLpct", "WinLoss", "career_team_gp"
).join(org_diff_stats, Seq("org")).withColumn(
	"career_team_avg_pts", $"career_team_avg_pts" - $"cta_pts"
).withColumn(
	"career_team_avg_ftm", $"career_team_avg_ftm" - $"cta_ftm"
).withColumn(
	"career_team_avg_fta", $"career_team_avg_fta" - $"cta_fta"
).withColumn(
	"career_team_avg_pf", $"career_team_avg_pf" - $"cta_pf"
).withColumn(
	"career_team_avg_reb", $"career_team_avg_reb" - $"cta_reb"
).withColumn(
	"career_team_avg_ast", $"career_team_avg_ast" - $"cta_ast"
).withColumn(
	"career_team_avg_tov", $"career_team_avg_tov" - $"cta_tov"
).withColumn(
	"career_team_avg_opppts", $"career_team_avg_pts" - $"cta_pts"
).withColumn(
	"career_team_avg_oppftm", $"career_team_avg_oppftm" - $"cta_oppftm"
).withColumn(
	"career_team_avg_oppfta", $"career_team_avg_oppfta" - $"cta_oppfta"
).withColumn(
	"career_team_avg_opppf", $"career_team_avg_opppf" - $"cta_opppf"
).withColumn(
	"WLpct", $"WLpct" - $"cta_WLpct"
).withColumn(
	"WinLoss", concat_ws(" ||| ",$"WinLoss", $"cta_WinLoss")
).drop(
	"cta_pts", "cta_ftm", "cta_fta", "cta_pf", "cta_reb", "cta_ast", "cta_tov", "cta_opppts",
	"cta_oppftm", "cta_oppfta", "cta_opppf", "cta_WLpct", "cta_WinLoss"
)

// writing all the data to csvs

val refplayer_write = ref_player_comps.coalesce(1).write.format(
	"com.databricks.spark.csv").option("header", "true"
).save("ref_player")

val reforg_write = ref_org_comps.coalesce(1).write.format(
	"com.databricks.spark.csv").option("header", "true"
).save("ref_org")

val player_write = player_stats.coalesce(1).write.format(
	"com.databricks.spark.csv").option("header", "true"
).save("player_stats")

val org_write = org_stats.coalesce(1).write.format(
	"com.databricks.spark.csv").option("header", "true"
).save("org_stats")

val playerdiff_write = player_diff.coalesce(1).write.format(
	"com.databricks.spark.csv").option("header", "true"
).save("player_diff")

val orgdiff_write = org_diff.coalesce(1).write.format(
	"com.databricks.spark.csv").option("header", "true"
).save("org_diff")






