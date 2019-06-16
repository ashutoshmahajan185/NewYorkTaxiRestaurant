import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.regression.{RandomForestRegressor, LinearRegression}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType,DoubleType};
import org.apache.spark.sql.Row;

def getTime(t:String): String = {
    val hour = t.split(":")(0).toInt
    var mins = t.split(":")(1).toInt
    var converted = (mins/15) * 15
    val time: String = hour+":"+converted
    return time
}

// Converting from the raw date format to Day of week format
def getDay(date: String): String = {

    import java.time.LocalDate
    import java.time.format.DateTimeFormatter
    //to remove "(" at the start"
    val t = date.substring(1,date.length)
    val d = t.split("-")
    if(d(2).length == 1)
    d(2) = "0" + d(2)
    if(d(1).length == 1)
    d(1) = "0" + d(1)
    val parsedDate = d(2) + "/" + d(1) + "/" + d(0)
    val dtf = DateTimeFormatter.ofPattern("dd/MM/yyyy")
    val dayOfWeek = LocalDate.parse(parsedDate,dtf).getDayOfWeek
    val ret = dayOfWeek + ""
    return ret

}

def degToRad(degree: Double): Double = {
return degree * (Math.PI/180)
}

def getDisplacement(lat1: Double, lat2: Double, lon1: Double, lon2: Double): Double = {
val radius = 6371;
val disLat = degToRad(lat2-lat1);
val disLon = degToRad(lon2-lon1);
val temp1 = Math.sin(disLat/2) * Math.sin(disLat/2) + Math.cos(degToRad(lat1)) * Math.cos(degToRad(lat2)) * Math.sin(disLon/2) * Math.sin(disLon/2); 
val temp2 = 2 * Math.atan2(Math.sqrt(temp1), Math.sqrt(1-temp1)); 
var distance = radius * temp2;
return distance;
}

val rdd = sc.textFile("/user/sjd451/project/cleanTaxiData")
val rdd1 = rdd.map(line => line.split(",")).filter(line => !line.isEmpty)
// day time lat long lat long fare 
val pickups = rdd1.map(line => getDay(line(0).split(" ")(0)) + "," + getTime(line(0).split(" ")(1)) + "," + line(4) + "," + line(5) + "," + line(6) + "," + line(7) + "," + line(8).substring(0, line(8).length-1)).map(line => line.split(","))

val rdd = pickups.map(row => Row(row(0),row(1), row(2).toDouble, row(3).toDouble, row(4).toDouble, row(5).toDouble, row(6).toDouble))

val schema = new StructType().add(StructField("Day", StringType, true)).add(StructField("Time", StringType, true)).add(StructField("Pick-lat", DoubleType, true)).add(StructField("Pick-lon", DoubleType, true)).add(StructField("Drop-lat", DoubleType, true)).add(StructField("Drop-lon", DoubleType, true)).add(StructField("label", DoubleType, true))
var dataframe = sqlContext.createDataFrame(rdd, schema)

val timeIndexer = new StringIndexer().setInputCol("Time").setOutputCol("TimeIndexed")
val dayIndexer = new StringIndexer().setInputCol("Day").setOutputCol("DayIndexed")
val encoder = new OneHotEncoder().setInputCol("DayIndexed").setOutputCol("DayEncoded")
val encoder2 = new OneHotEncoder().setInputCol("TimeIndexed").setOutputCol("TimeEncoded")
dataframe = timeIndexer.fit(dataframe).transform(dataframe)
dataframe = dayIndexer.fit(dataframe).transform(dataframe)
dataframe = encoder.transform(dataframe)
dataframe = encoder2.transform(dataframe)

var assembler = new VectorAssembler().setInputCols(Array("Pick-lat", "Pick-lon", "Drop-lon", "Drop-lat", "TimeEncoded", "DayEncoded")).setOutputCol("features") 
dataframe = assembler.transform(dataframe)

var Array(train,test) = dataframe.randomSplit(Array(.8, .2), 42)

var lr = new LinearRegression().setRegParam(0.01)
var lrModel = lr.fit(train)
var lrPrediction = lrModel.transform(test)

val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("label").setPredictionCol("prediction")
val rmse = evaluator.evaluate(lrPrediction)

println(s"Root-mean-square error = $rmse")

var lr = new org.apache.spark.mllib.regression.RidgeRegressionWithSGD()
var lrModel = lr.fit(train)
var lrPrediction = lrModel.transform(test)

val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("label").setPredictionCol("prediction")
val rmse = evaluator.evaluate(lrPrediction)

println(s"Root-mean-square error = $rmse")



import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.Pipeline
val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(dataframe)
val rf = new RandomForestRegressor().setLabelCol("label").setFeaturesCol("indexedFeatures")
// Chain indexer and forest in a Pipeline.
val pipeline = new Pipeline().setStages(Array(featureIndexer, rf))

// Train model. This also runs the indexer.
val rfmodel = pipeline.fit(train)

// Make predictions.
val predictions = rfmodel.transform(test)
val rmse = evaluator.evaluate(predictions)
// Select example rows to display.
predictions.select("prediction", "label", "features").show(5)







// weekday time lat lon fare dist date

val pickups = rdd1.map(line => getDay(line(0).split(" ")(0)) + "," + getTime(line(0).split(" ")(1)) + "," + line(4) + "," + line(5) + "," + line(6) + "," + line(7) + "," + line(8).substring(0, line(8).length-1) + "," + getDisplacement(line(5).toDouble, line(7).toDouble, line(4).toDouble, line(6).toDouble) + "," + line(0).split(" ")(0).substring(1, line(0).split(" ")(0).length)).map(line => line.split(","))

val rdd = pickups.map(row => Row(row(0),row(1), row(2).toDouble, row(3).toDouble, row(4).toDouble, row(5).toDouble, row(6).toDouble, row(7).toDouble, row(8)))

val schema = new StructType().add(StructField("Day", StringType, true)).add(StructField("Time", StringType, true)).add(StructField("Pick-lat", DoubleType, true)).add(StructField("Pick-lon", DoubleType, true)).add(StructField("Drop-lat", DoubleType, true)).add(StructField("Drop-lon", DoubleType, true)).add(StructField("label", DoubleType, true)).add(StructField("Dist", DoubleType, true)).add(StructField("Date", StringType, true))
var dataframe = sqlContext.createDataFrame(rdd, schema)

val timeIndexer = new StringIndexer().setInputCol("Time").setOutputCol("TimeIndexed")
val dayIndexer = new StringIndexer().setInputCol("Day").setOutputCol("DayIndexed")
val dateIndexer = new StringIndexer().setInputCol("Day").setOutputCol("DateIndexed")
val encoder = new OneHotEncoder().setInputCol("DayIndexed").setOutputCol("DayEncoded")
val encoder2 = new OneHotEncoder().setInputCol("TimeIndexed").setOutputCol("TimeEncoded")
dataframe = timeIndexer.fit(dataframe).transform(dataframe)
dataframe = dateIndexer.fit(dataframe).transform(dataframe)
dataframe = dayIndexer.fit(dataframe).transform(dataframe)
dataframe = encoder.transform(dataframe)
dataframe = encoder2.transform(dataframe)

var assembler = new VectorAssembler().setInputCols(Array("Dist","Pick-lat", "Pick-lon", "Drop-lon", "Drop-lat","DateIndexed", "TimeEncoded", "DayEncoded")).setOutputCol("features") 
dataframe = assembler.transform(dataframe)

var Array(train,test) = dataframe.randomSplit(Array(.8, .2), 42)

var lr = new LinearRegression().setRegParam(0.01)
var lrModel = lr.fit(train)
var lrPrediction = lrModel.transform(test)

val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("label").setPredictionCol("prediction")
val rmse = evaluator.evaluate(lrPrediction)

println(s"Root-mean-square error = $rmse")

