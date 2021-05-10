// Seller Ranking:
// ---------------

:load 1_load_data.scala

val ePairRdd21 = ePairRdd.map{ case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,
       oStat,pWt,pLen,pHt,pWidth,cCity,
       cState,sId,sCity,sState,sInstal) => (sId,1) }.reduceByKey(_+_).sortBy(_._2,false)

val ePairRdd22 = ePairRdd.map{ case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,
       oStat,pWt,pLen,pHt,pWidth,cCity,
       cState,sId,sCity,sState,sInstal) => (sId,sp*qt) }.reduceByKey(_+_).sortBy(_._2,false)

val ePairRdd23 = ePairRdd.map{ case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,
       oStat,pWt,pLen,pHt,pWidth,cCity,
       cState,sId,sCity,sState,sInstal) => (sId,rating) }.reduceByKey(_+_).sortBy(_._2,false)

// Joining 3 RDDs
val ePairRdd24 = (ePairRdd21.join(ePairRdd22)).join(ePairRdd23)
ePairRdd24.toDF.show

val ePairRdd25 = ePairRdd24.map{ case(sId,q_p_r)=>
      val q_p = q_p_r._1
      val qt = q_p._1
      val price = q_p._2
      val rating_all = q_p_r._2
      val rating = rating_all.toFloat/qt.toFloat
      val rating_roundoff = (BigDecimal(rating).setScale(2, BigDecimal.RoundingMode.HALF_UP)).toFloat
      (sId,qt,price,rating_roundoff) }.sortBy(_._3,false)
ePairRdd25.toDF("Seller ID","Customers Reached","Total Sales","Avg Rating").show

/*
+--------------------+-----------------+-----------+----------+
|           Seller ID|Customers Reached|Total Sales|Avg Rating|
+--------------------+-----------------+-----------+----------+
|7c67e1448b00f6e96...|              170|   186788.0|      2.84|
|1f50f920176fa81da...|              189|   142442.4|      4.14|
|955fee9216a65b617...|              232|  102963.87|      3.71|
|1025f0e2d44d7041d...|              238|    76052.0|      3.48|
|52d76513f0c4d97f3...|               12|  73640.055|       3.0|
|b37c4c02bda3161a7...|                4|    72748.8|       1.0|
|9803a40e82e45418a...|               11|   67857.95|       2.0|
|7681ef142fd2c1904...|               28|   62967.66|      4.29|
|fcdd820084f17e998...|               13|   51271.54|      2.08|
|de722cd6dad950a92...|               70|   45660.85|      4.06|
|e7d5b006eb624f130...|               14|   44268.84|      2.86|
|ca3bd7cd9f149df75...|               21|    43684.7|       3.1|
|6560211a19b47992c...|              316|  34781.816|      3.58|
|da8622b14eb17ae28...|              128|    33521.6|       4.0|
|7d13fca1522535862...|              158|   33313.93|      3.74|
|f7ba60f8c3f99e7ee...|               28|    32147.6|       4.0|
|634964b17796e6430...|               57|  31873.389|      3.68|
|4a3ca9315b744ce9f...|              145|   30838.37|      3.77|
|c70c1b0d8ca86052f...|               86|   29413.35|      3.63|
|1a932caad4f9d8040...|               12|   28909.49|      3.17|
+--------------------+-----------------+-----------+----------+
*/

val file = new java.io.PrintStream("BDA_Project\\seller_rank.csv")
ePairRdd25.collect.foreach ( file.println(_) )
file.close