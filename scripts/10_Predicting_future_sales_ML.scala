// Predicting future sales:
// ------------------------

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.Normalizer

val df = sc.parallelize(Array( (1,10),(2,20),(3,30),(4,40) )).toDF("label","feature_1")

val assembler1 = new VectorAssembler().
  setInputCols(Array("feature_1")).
  setOutputCol("features").
  transform(df)
assembler1.show()

/*
+-----+---------+--------+
|label|feature_1|features|
+-----+---------+--------+
|    1|       10|  [10.0]|
|    2|       20|  [20.0]|
|    3|       30|  [30.0]|
|    4|       40|  [40.0]|
+-----+---------+--------+
*/

val normalizer = new Normalizer().
  setInputCol("features").
  setOutputCol("normFeatures").
  setP(2.0).
  transform(assembler1)
normalizer.show()

/*
+-----+---------+--------+------------+
|label|feature_1|features|normFeatures|
+-----+---------+--------+------------+
|    1|       10|  [10.0]|       [1.0]|
|    2|       20|  [20.0]|       [1.0]|
|    3|       30|  [30.0]|       [1.0]|
|    4|       40|  [40.0]|       [1.0]|
+-----+---------+--------+------------+
*/

val Array(trainingData, testData) = assembler1.randomSplit(Array(0.75, 0.25))

trainingData.show

/*
+-----+---------+--------+
|label|feature_1|features|
+-----+---------+--------+
|    1|       10|  [10.0]|
|    2|       20|  [20.0]|
|    3|       30|  [30.0]|
+-----+---------+--------+
*/

testData.show

/*
+-----+---------+--------+
|label|feature_1|features|
+-----+---------+--------+
|    4|       40|  [40.0]|
+-----+---------+--------+
*/

val lr = new LinearRegression().
  setLabelCol("label").
  setFeaturesCol("features").
  setMaxIter(10).
  setRegParam(1.0).
  setElasticNetParam(1.0)

val lrModel = lr.fit(trainingData)

lrModel.
transform(testData).
select("features","label","prediction").
show()

/*
+--------+-----+----------+
|features|label|prediction|
+--------+-----+----------+
|  [40.0]|    4|       2.0|
+--------+-----+----------+
*/

println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
/*
Coefficients: [0.0] Intercept: 2.0
*/

val trainingSummary = lrModel.summary
println(s"numIterations: ${trainingSummary.totalIterations}")
/*
numIterations: 1
*/

println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
/*
objectiveHistory: [0.5]
*/

trainingSummary.residuals.show()

/*
+---------+
|residuals|
+---------+
|     -1.0|
|      0.0|
|      1.0|
+---------+
*/

println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
/*
RMSE: 0.8164965809277261
*/

println(s"r2: ${trainingSummary.r2}")
/*
r2: -2.220446049250313E-16
*/


// Mannual Testing:

val test_df = sc.parallelize(Array( (6,60),(7,70),(8,80) )).toDF("label","feature_1")

val test_assembler = new VectorAssembler().
  setInputCols(Array("feature_1")).
  setOutputCol("features").
  transform(test_df)

lrModel.
transform(test_assembler).
select("features","label","prediction").
show()

/*
+--------+-----+----------+
|features|label|prediction|
+--------+-----+----------+
|  [60.0]|    6|       2.0|
|  [70.0]|    7|       2.0|
|  [80.0]|    8|       2.0|
+--------+-----+----------+
*/

val file = new java.io.PrintStream("BDA_Project\\hourly_data.csv")
all_customers_count.collect.foreach ( file.println(_) )
file.close