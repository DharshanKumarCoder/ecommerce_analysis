// Product Wise Analysis:
// ----------------------

:load 1_load_data.scala

val prodRdd=ePairRdd.map{case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,oStat,pWt,pLen,pHt,pWidth,cCity,cState,sId,sCity,sState,sInstal)=>
      (pId,(List(rating),List(sp)))}.reduceByKey{case(a,b)=>(a._1++b._1,a._2++b._2)}.map{case(a,(b,c))=>(a,b.sum*1.0/b.length,c.sum/c.length,c.length)}

// Total number of unique products sold:
prodRdd.count
//res28: Long = 6068

prodRdd.toDF("Product ID","Avg Rating","Avg Sales","No. of Orders").show
/*
+--------------------+-----------------+---------+-------------+
|          Product ID|       Avg Rating|Avg Sales|No. of Orders|
+--------------------+-----------------+---------+-------------+
|2196812e80b905c70...|              5.0|   141.97|            1|
|e24f73b7631ee3fbb...|              4.0|    136.6|            2|
|12fc9ab82dd45f382...|              4.5|    21.84|            2|
|486cebbfac430b06c...|              1.0|   232.88|            1|
|04c31b0da644f1782...|              5.0|    79.23|            1|
|9680a78df870a1cb6...|              4.0|   108.63|            1|
|f34152d311875e9da...|              5.0|    46.61|            1|
|b914a18d4547eff29...|              1.0|   767.33|            1|
|af407c297ba8ec158...|              5.0|   144.46|            1|
|192d363be840c4c30...|              5.0|    66.71|            1|
|b05fae603a3a28a97...|              4.0|    316.4|            1|
|31c79131e883e5fd8...|              5.0|   173.76|            2|
|ee08e2b1f2f38ce75...|              5.0|    51.06|            1|
|02a97df83a8a100c7...|              1.0|    78.72|            1|
|176faef125e3675b4...|              1.0|    44.27|            1|
|3ecdc0c6f18cf5c74...|              5.0|   149.01|            1|
|4ecb94d07e0dc5367...|              5.0|   264.33|            1|
|da1415c61b55b4e9e...|4.333333333333333|   161.27|            3|
|a6a8b6b05afd3a6e6...|              1.0|   109.24|            1|
|12803e6c487560862...|              5.0|  2223.12|            1|
+--------------------+-----------------+---------+-------------+
*/

val pSorted=prodRdd.sortBy( x => (x._4), ascending = true).collect.toList

// Top-10 Most Sold products:
pSorted.reverse.toDF("Product ID","Avg Rating","Avg Sales","No. of Orders").show
/*
+--------------------+------------------+---------+-------------+
|          Product ID|        Avg Rating|Avg Sales|No. of Orders|
+--------------------+------------------+---------+-------------+
|53b36df67ebb7c415...|3.9859154929577465|126.67379|           71|
|3fbc0ef745950c793...|3.3666666666666667| 111.7982|           60|
|19c91ef95d509ea33...| 4.033898305084746|156.36154|           59|
|422879e10f4668299...|             3.875|151.90225|           48|
|a92930c327948861c...|3.2954545454545454| 88.88952|           44|
|3dd2a17168ec895c7...|3.9069767441860463| 292.0474|           43|
|d1c427060a0f73f6b...|4.2439024390243905|173.98273|           41|
|a62e25e09e05e6faf...|             3.675|  300.078|           40|
|aca2eb7d00ea1a7b8...| 3.871794871794872|114.70897|           39|
|89b121bee266dcd25...|1.7222222222222223|100.56999|           36|
|d285360f29ac7fd97...|2.7142857142857144|241.91881|           35|
|e0d64dcfaa3b6db5c...|               2.6|152.95113|           35|
|368c6c730842d7801...| 4.735294117647059|140.83795|           34|
|99a4788cb24856965...| 4.121212121212121|141.62604|           33|
|4fe644d766c7566db...|            3.4375|111.36062|           32|
|fbce4c4cb307679d8...| 4.064516129032258|178.88127|           31|
|e7cc48a9daff5436f...|2.7666666666666666|283.18268|           30|
|389d119b48cf3043d...| 4.482758620689655|143.23035|           29|
|928e52a9ad53a294f...|2.8620689655172415|505.98178|           29|
|bb50f2e236e5eea01...|              4.24|  548.052|           25|
+--------------------+------------------+---------+-------------+
*/

val file = new java.io.PrintStream("BDA_Project\\Category_analysis.csv")
prodRdd.sortBy(_._4,false).collect.foreach ( x => file.println(x._1+","+x._2+","+x._3+","+x._4) )
file.close

// Least-10 sold products:
pSorted.toDF("Product ID","Avg Rating","Avg Sales","No. of Orders").show
/*
+--------------------+----------+---------+-------------+
|          Product ID|Avg Rating|Avg Sales|No. of Orders|
+--------------------+----------+---------+-------------+
|2196812e80b905c70...|       5.0|   141.97|            1|
|486cebbfac430b06c...|       1.0|   232.88|            1|
|04c31b0da644f1782...|       5.0|    79.23|            1|
|9680a78df870a1cb6...|       4.0|   108.63|            1|
|f34152d311875e9da...|       5.0|    46.61|            1|
|b914a18d4547eff29...|       1.0|   767.33|            1|
|af407c297ba8ec158...|       5.0|   144.46|            1|
|192d363be840c4c30...|       5.0|    66.71|            1|
|b05fae603a3a28a97...|       4.0|    316.4|            1|
|ee08e2b1f2f38ce75...|       5.0|    51.06|            1|
|02a97df83a8a100c7...|       1.0|    78.72|            1|
|176faef125e3675b4...|       1.0|    44.27|            1|
|3ecdc0c6f18cf5c74...|       5.0|   149.01|            1|
|4ecb94d07e0dc5367...|       5.0|   264.33|            1|
|a6a8b6b05afd3a6e6...|       1.0|   109.24|            1|
|12803e6c487560862...|       5.0|  2223.12|            1|
|f2a286e75c6c6022c...|       1.0|   191.79|            1|
|481a7cf644601696c...|       4.0|   209.78|            1|
|b9732f4c0f2003913...|       3.0|   138.25|            1|
|5f82c0dd37c20e3f5...|       5.0|   313.67|            1|
+--------------------+----------+---------+-------------+
*/


// Product Category Analysis:
// --------------------------

val pCatRdd=ePairRdd.map{case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,oStat,pWt,pLen,pHt,pWidth,cCity,cState,sId,sCity,sState,sInstal)=>
      (pCat,(List(rating),List(sp)))}.reduceByKey{case(a,b)=>(a._1++b._1,a._2++b._2)}.map{case(a,(b,c))=>(a,b.sum*1.0/b.length,c.sum/c.length,c.length)}

//Total number of unique product categories sold
pCatRdd.count
//res23: Long = 66

pCatRdd.toDF("Product Category","Avg Rating","Avg Sales","No. of Orders").show
/*
+--------------------+------------------+----------+-------------+
|    Product Category|        Avg Rating| Avg Sales|No. of Orders|
+--------------------+------------------+----------+-------------+
|      party_supplies|3.1666666666666665| 99.714165|           12|
|        dvds_blu_ray|               2.0| 87.973335|            3|
|agro_industry_and...|               3.6| 394.45065|           15|
|   cds_dvds_musicals|               5.0|    117.58|            1|
|           computers|              3.96| 1895.7657|           25|
|books_general_int...| 4.085106382978723|113.190865|           47|
|      bed_bath_table|3.5972558514931396| 158.00053|         1239|
|           telephony| 3.719730941704036| 144.22685|          446|
|fashion_childrens...|               5.0|     97.48|            1|
| diapers_and_hygiene|               3.5|    74.175|            4|
|               audio|3.8095238095238093| 139.87924|           42|
|     books_technical|               3.9| 72.131004|           40|
|    small_appliances|3.6779661016949152| 357.91623|           59|
|fashion_underwear...| 4.571428571428571|  88.44857|            7|
|     home_appliances|3.9831932773109244|   89.1548|          119|
|                food| 4.029850746268656|  87.38002|           67|
|          stationery| 4.278301886792453| 131.34314|          212|
|          cool_stuff|  3.89237668161435| 229.74574|          223|
|small_appliances_...| 4.266666666666667| 481.29736|           15|
|  christmas_supplies| 4.533333333333333| 89.307335|           15|
+--------------------+------------------+----------+-------------+
*/

val cSorted=pCatRdd.sortBy( x => (x._4), ascending = true).collect.toList

//Top 10 categories sold
cSorted.reverse.toDF("Product Category","Avg Rating","Avg Sales","No. of Orders").show
/*
+--------------------+------------------+----------+-------------+
|    Product Category|        Avg Rating| Avg Sales|No. of Orders|
+--------------------+------------------+----------+-------------+
|      bed_bath_table|3.5972558514931396| 158.00053|         1239|
|       health_beauty|3.8967052537845057| 179.26001|         1123|
|          housewares|3.6645569620253164| 184.62833|          948|
|       watches_gifts| 3.685774946921444| 220.68384|          942|
|      sports_leisure|3.9197860962566846| 158.82326|          748|
|     furniture_decor| 3.590659340659341| 199.38974|          728|
|computers_accesso...| 3.654879773691655| 188.66545|          707|
|                auto| 3.810313075506446| 190.96826|          543|
|           telephony| 3.719730941704036| 144.22685|          446|
|        garden_tools|            4.0625| 195.06085|          384|
|                baby|3.6064139941690962| 216.75752|          343|
|         electronics|3.9792387543252596|105.032196|          289|
|           perfumery| 4.020833333333333| 160.43077|          288|
|                toys| 3.928030303030303| 177.35278|          264|
|            pet_shop|4.0627450980392155| 177.15346|          255|
|          cool_stuff|  3.89237668161435| 229.74574|          223|
|          stationery| 4.278301886792453| 131.34314|          212|
|    office_furniture| 3.018957345971564|    461.32|          211|
|construction_tool...|3.7923497267759565| 268.34167|          183|
|fashion_bags_acce...| 4.073033707865169| 139.55518|          178|
+--------------------+------------------+----------+-------------+
*/

val file = new java.io.PrintStream("BDA_Project\\Category_analysis.csv")
pCatRdd.sortBy(_._4,false).collect.foreach ( x => file.println(x._1+","+x._2+","+x._3+","+x._4) )
file.close

//Least 10 categories sold
cSorted.toDF("Product Category","Avg Rating","Avg Sales","No. of Orders").show
/*
+--------------------+------------------+---------+-------------+
|    Product Category|        Avg Rating|Avg Sales|No. of Orders|
+--------------------+------------------+---------+-------------+
|   cds_dvds_musicals|               5.0|   117.58|            1|
|fashion_childrens...|               5.0|    97.48|            1|
|             flowers|               3.0|    35.72|            1|
|tablets_printing_...|               5.0|    58.19|            1|
|        dvds_blu_ray|               2.0|87.973335|            3|
| diapers_and_hygiene|               3.5|   74.175|            4|
|      books_imported|              3.75| 221.2775|            4|
|      home_comfort_2|               2.0|52.300003|            4|
|fashion_underwear...| 4.571428571428571| 88.44857|            7|
|arts_and_craftman...|               3.0|111.47876|            8|
|               music|3.7777777777777777| 252.8711|            9|
|fashion_male_clot...| 4.333333333333333|123.42113|            9|
|      party_supplies|3.1666666666666665|99.714165|           12|
|          cine_photo| 4.153846153846154|117.23462|           13|
|   furniture_bedroom| 4.153846153846154| 260.3946|           13|
|agro_industry_and...|               3.6|394.45065|           15|
|small_appliances_...| 4.266666666666667|481.29736|           15|
|  christmas_supplies| 4.533333333333333|89.307335|           15|
|costruction_tools...| 4.266666666666667|310.21066|           15|
|       fashion_shoes|              4.75|163.08998|           16|
+--------------------+------------------+---------+-------------+
*/


//Ratings given
val ratingRdd=ePairRdd.map{case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,oStat,pWt,pLen,pHt,pWidth,cCity,cState,sId,sCity,sState,sInstal)=>
      (rating,1)}.reduceByKey(_+_)
//ratingRdd: org.apache.spark.rdd.RDD[(Int, Int)] = ShuffledRDD[98] at reduceByKey at <console>:26

ratingRdd.sortBy(x=>x._1).toDF("Rating","Count of Ratings").show
/*
+------+----------------+
|Rating|Count of Ratings|
+------+----------------+
|     1|            2244|
|     2|             609|
|     3|             945|
|     4|            1743|
|     5|            6190|
+------+----------------+
*/
