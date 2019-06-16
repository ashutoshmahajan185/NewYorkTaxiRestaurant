import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.regression.{RandomForestRegressor, LinearRegression}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType,DoubleType};
import org.apache.spark.sql.Row;
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.Pipeline


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

val rdd = sc.textFile("/user/sjd451/project/cleanTaxiData")
val rdd1 = rdd.map(line => line.split(",")).filter(line => !line.isEmpty)
val pickups = rdd1.map(line => getDay(line(0).split(" ")(0)) + "," + getTime(line(0).split(" ")(1)) + "," + line(4) + "," + line(5) + "," + line(6) + "," + line(7) + "," + line(8).substring(0, line(8).length-1) + "," + line(3)).map(line => line.split(","))
val rdd = pickups.map(row => Row(row(0),row(1), row(2).toDouble, row(3).toDouble, row(4).toDouble, row(5).toDouble, row(6).toDouble, row(7).toDouble))
val schema = new StructType().add(StructField("Day", StringType, true)).add(StructField("Time", StringType, true)).add(StructField("Pick-lat", DoubleType, true)).add(StructField("Pick-lon", DoubleType, true)).add(StructField("Drop-lat", DoubleType, true)).add(StructField("Drop-lon", DoubleType, true)).add(StructField("label", DoubleType, true)).add(StructField("Dist", DoubleType, true))
var dataframe = sqlContext.createDataFrame(rdd, schema)

val timeIndexer = new StringIndexer().setInputCol("Time").setOutputCol("TimeIndexed")
val dayIndexer = new StringIndexer().setInputCol("Day").setOutputCol("DayIndexed")
val encoder = new OneHotEncoder().setInputCol("DayIndexed").setOutputCol("DayEncoded")
val encoder2 = new OneHotEncoder().setInputCol("TimeIndexed").setOutputCol("TimeEncoded")
dataframe = timeIndexer.fit(dataframe).transform(dataframe)
dataframe = dayIndexer.fit(dataframe).transform(dataframe)
dataframe = encoder.transform(dataframe)
dataframe = encoder2.transform(dataframe)

var assembler = new VectorAssembler().setInputCols(Array("Dist","Pick-lat", "Pick-lon", "Drop-lon", "Drop-lat", "TimeEncoded", "DayEncoded")).setOutputCol("features") 
dataframe = assembler.transform(dataframe)

// Automatically identify categorical features, and index them.
// Set maxCategories so features with > 4 distinct values are treated as continuous.
val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(dataframe)
var Array(train,test) = dataframe.randomSplit(Array(.7, .3), 42)
// Chain indexer and forest in a Pipeline.
val pipeline = new Pipeline().setStages(Array(featureIndexer, rf))

// Split the data into training and test sets (30% held out for testing).
val Array(trainingData, testData) = dataframe.randomSplit(Array(0.7, 0.3))

// Train a GBT model.
val gbt = new GBTRegressor().setLabelCol("label").setFeaturesCol("indexedFeatures").setMaxIter(10)

// Chain indexer and GBT in a Pipeline.
val pipeline = new Pipeline().setStages(Array(featureIndexer, gbt))

// Train model. This also runs the indexer.
val model = pipeline.fit(trainingData)
sc.parallelize(Seq(model), 1).saveAsObjectFile("/user/sjd451/project/gbrmodel")
// Make predictions.
val predictions = model.transform(testData)

// Select example rows to display.
predictions.select("prediction", "label", "features").show(5)

// Select (prediction, true label) and compute test error.
val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")
val rmse = evaluator.evaluate(predictions)
println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

val gbtModel = model.stages(1).asInstanceOf[GBTRegressionModel]
println(s"Learned regression GBT model:\n ${gbtModel.toDebugString}")