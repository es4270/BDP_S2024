import org.apache.spark.sql.DataFrame

val df_ref_player = spark.read.format(
	"csv"
).option(
	"header", "true"
).option(
	"inferschema", "true"
).load("hdfs://nyu-dataproc-m/user/es4270_nyu_edu/ref_player")

val df_ref_org = spark.read.format(
	"csv"
).option(
	"header", "true"
).option(
	"inferschema", "true"
).load("hdfs://nyu-dataproc-m/user/es4270_nyu_edu/ref_org")

val df_player = spark.read.format(
	"csv"
).option(
	"header", "true"
).option(
	"inferschema", "true"
).load("hdfs://nyu-dataproc-m/user/es4270_nyu_edu/player_stats")

val df_org = spark.read.format(
	"csv"
).option(
	"header", "true"
).option(
	"inferschema", "true"
).load("hdfs://nyu-dataproc-m/user/es4270_nyu_edu/org_stats")

val df_playerdiff = spark.read.format(
	"csv"
).option(
	"header", "true"
).option(
	"inferschema", "true"
).load("hdfs://nyu-dataproc-m/user/es4270_nyu_edu/player_diff")

val df_orgdiff = spark.read.format(
	"csv"
).option(
	"header", "true"
).option(
	"inferschema", "true"
).load("hdfs://nyu-dataproc-m/user/es4270_nyu_edu/org_diff")


//shows profile of one col
def colsum (cleaned: DataFrame, colname: String): (Array[String]) = {

	//use built in scala describe to get mean lol
	var mean = cleaned.describe(colname).collect()(1)(1)
	var min = cleaned.describe(colname).collect()(3)(1)
	var max = cleaned.describe(colname).collect()(4)(1)

	//use built in scala describe to get stddev lol
	var stddev = cleaned.describe(colname).collect()(2)(1)

	//get mode by grouping a column by its count, ordering it from highest
	//count to least, and then getting the first one
	var mode = cleaned.groupBy(colname).count().orderBy(desc("count")).first()
	
	//getting the median by selecting the column, using stat to get an array of
	//the values, taking half the array, and taking the head
	var median = cleaned.select(colname)
		.stat.approxQuantile(colname, Array(0.5), 0.001).head

	//adding all of these to strings for nice presentaiton
	var countstr = colname + " count: " + cleaned.count().toString()
	var meanstr = colname + " mean: " + mean.toString()
	var stddevstr = colname + " stddev: " + stddev.toString()
	var modestr = colname + " mode: " + mode(0) + ", occurences: " + mode(1)
	var medianstr = median.toString()
	var minstr = colname + " min: " + min.toString()
	var maxstr = colname + " max: " + max.toString()
	medianstr = colname + " median: "+ medianstr

	//adding these strings to an output array, then outputting them
	var out = Array(meanstr, stddevstr, modestr, medianstr, minstr, maxstr)
	(out)
}

//shows profile of cols using above functions
def colprofile (cleaned: DataFrame, dfname: String): Unit = {
	println("====================")
	println(dfname + " Column Summary")
	println("====================")
	for(ele <- cleaned.columns.toSeq){
		if(cleaned.dtypes.filter(_._1 == ele).map(_._2).headOption.getOrElse(None) == "StringType") {
			println("====================")
			println(dfname + " " + ele + " column summary")
			cleaned.groupBy(ele).count().describe().collect().foreach(println)
			println("====================")
		}else{
			println("====================")
			println(dfname + " " + ele + " column summary")
			colsum(cleaned,ele).foreach(println)
			println("====================")
		}
	}
}

//use showmin and showmax to show min/max 10 by whatever column
def showmin (df: DataFrame, colname: String): Unit = df.orderBy(colname).show(10)
def showmax (df: DataFrame, colname: String): Unit = df.orderBy(desc(colname)).show(10)

//select a player
def plyrselect(df: DataFrame, playername: String): (DataFrame) = {
	df.filter(df("player_name") === playername)
}

//select an org
def orgselect(df: DataFrame, orgname: String): (DataFrame) = {
	df.filter(df("org") === orgname)
}

//sample
showmin(
	plyrselect(
		df_ref_player, "Chris Paul"
	).select("player_name","official_name"), 
	"WLpct"
)

colprofile(plyrselect(df_ref_player,"Stephen Curry"), "Stephen Curry Ref")





