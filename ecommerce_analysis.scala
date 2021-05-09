val eRdd = sc.textFile("ecom_data.csv")
eRdd.count

// To remove 1st header row
val eRdd1 = eRdd.mapPartitionsWithIndex {
       (idx, iter) => if (idx == 0) iter.drop(1) else iter
       }

val ePairRdd = eRdd1.map{ l =>
      val str0 = l.split(',')
      val (oid,cid,qt,cp,sp) = (str0(0),str0(1),str0(2).toInt,str0(3).toFloat,str0(4).toFloat)
      val (ts,rating,pCat,pId,sp_type) = (str0(5),str0(6).toInt,str0(7),str0(8),str0(9))
      val (oStat,pWt,pLen,pHt,pWidth) = (str0(10),str0(11).toInt,str0(12).toInt,str0(13).toInt,str0(14).toInt)
      val (cCity,cState,sId,sCity,sState) = (str0(15),str0(16),str0(17),str0(18),str0(19))
      val (sInstal) = (str0(20).toInt)
      
      (oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,
       oStat,pWt,pLen,pHt,pWidth,cCity,
       cState,sId,sCity,sState,sInstal) }

ePairRdd.toDF("oid","cid","qt","cp","sp","ts","rating","pCat","pId","sp_type",
       "oStat","pWt","pLen","pHt","pWidth","cCity",
       "cState","sId","sCity","sState","sInstal").show

/*
+--------------------+--------------------+--------+---------+-------+----------------+------+----------------+--------------------+------------+------------+----------------+-----------------+-----------------+----------------+--------------------+--------------+--------------------+--------------------+------------+--------------------+
|            oid|         cid|qt|cp|sp|       ts|rating|pCat|          pId|sp_type|oStat|pWt|pLen|pHt|pWidth|       cCity|cState|           sId|         sCity|sState|sInstal|
+--------------------+--------------------+--------+---------+-------+----------------+------+----------------+--------------------+------------+------------+----------------+-----------------+-----------------+----------------+--------------------+--------------+--------------------+--------------------+------------+--------------------+
|d1ff908b4e21d4fff...|190508c583e9da289...|       1|    207.9| 238.61|04-08-2018 21:57|     5|   health_beauty|08462528607b71ea6...| credit_card|   delivered|             650|               16|               10|              11|sao goncalo do am...|            RN|ccc4bbb5f32a6ab2b...|            curitiba|          PR|                   8|
|093c11a6b12993f96...|77a6136f49bac0c33...|       1|    364.0| 797.38|13-06-2018 08:52|     5|   health_beauty|6cdd53843498f9289...| credit_card|   delivered|             900|               25|               12|              38|jaboatao dos guar...|            PE|ccc4bbb5f32a6ab2b...|            curitiba|          PR|                   6|
|093c11a6b12993f96...|77a6136f49bac0c33...|       2|    364.0| 797.38|13-06-2018 08:52|     5|   health_beauty|6cdd53843498f9289...| credit_card|   delivered|             900|               25|               12|              38|jaboatao dos guar...|            PE|ccc4bbb5f32a6ab2b...|            curitiba|          PR|                   6|
|d08bba6d656adbdc5...|2bbb7f863b68ba0f0...|       2|    364.0| 479.22|13-07-2018 20:40|     1|   health_beauty|6cdd53843498f9289...| credit_card|   delivered|             900|               25|               12|              38|      rio de janeiro|            RJ|ccc4bbb5f32a6ab2b...|            curitiba|          PR|                   2|
|8be26417e499e19c5...|08345c45f60f73773...|       1|    364.0|  428.8|05-08-2018 23:13|     5|   health_beauty|6cdd53843498f9289...| credit_card|   delivered|             900|               25|               12|              38|            toritama|            PE|ccc4bbb5f32a6ab2b...|            curitiba|          PR|                   8|
|0d711a2c41f1b9365...|213904d01203d2e28...|       1|    349.9| 365.43|24-04-2018 15:27|     5|   health_beauty|6cdd53843498f9289...| credit_card|   delivered|             900|               25|               12|              38|      rio de janeiro|            RJ|ccc4bbb5f32a6ab2b...|            curitiba|          PR|                   1|
|d7bcd768f38943877...|bce98ac8bc5881286...|       1|    349.9| 398.46|08-05-2018 09:22|     5|   health_beauty|6cdd53843498f9289...| credit_card|   delivered|             900|               25|               12|              38|              anadia|            AL|ccc4bbb5f32a6ab2b...|            curitiba|          PR|                   8|
|91d9b7669f0960199...|c9341f47a1c2979e0...|       1|    349.9| 370.52|10-04-2018 14:36|     5|   health_beauty|6cdd53843498f9289...| credit_card|   delivered|             900|               25|               12|              38|      rio de janeiro|            RJ|ccc4bbb5f32a6ab2b...|            curitiba|          PR|                   8|
+--------------------+--------------------+--------+---------+-------+----------------+------+----------------+--------------------+------------+------------+----------------+-----------------+-----------------+----------------+--------------------+--------------+--------------------+--------------------+------------+--------------------+
*/

// Customer Segmentation:
// ----------------------

val ePairRdd2 = ePairRdd.map{ case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,
       oStat,pWt,pLen,pHt,pWidth,cCity,
       cState,sId,sCity,sState,sInstal) => (cid,qt,sp) }

val ePairRdd4 = ePairRdd2.map{ case(cid,qt,sp) =>
      val amount = qt*sp
      val amount_roundoff = (math rint amount*100)/100
      (cid,amount_roundoff) }.reduceByKey(_+_).sortBy(_._2,false)
ePairRdd4.toDF("Customer ID","Bill Amount").show

/*
+--------------------+------------------+
|            Customer|       Bill Amount|
+--------------------+------------------+
|daf15f1b940cc6a72...|          97263.66|
|ec5b2ba62e5743423...|           72748.8|
|f7622098214b4634b...|          68099.64|
|71901689c5f3e5adc...|          67110.33|
|159132ab31eb3c8a1...|           45873.8|
|7d321bd4e8ba1caf7...|          44220.54|
|d95ca02ab50105ccc...|          35690.16|
|6152fbfc8a92ee25f...|31227.199999999997|
|222e4c4d91c814caf...|          29489.04|
|dd3f1762eb601f41c...|          28380.24|
|0179f2f4c32e0b0c2...|          27471.78|
|5c9d09439a7815d2c...|           26341.5|
|ce90096f130e38392...|          22130.64|
|0d554604c3b40dee6...|21498.120000000003|
|4a16dfed3e9e57741...|          21122.64|
|4dc417fbc348bf334...|          19339.74|
|9eb3d566e87289dcb...|          19033.56|
|94dd8366ec790734b...|           17362.8|
|a67a246af6ba598a1...|16322.320000000002|
|c0d62b7f7cbf132ac...|          16196.25|
+--------------------+------------------+
*/

val all_customers = ePairRdd4.map{ case (cid,amount) =>
      if (amount>=50000) {( "VIP customers", List((cid,amount)) )}
      else if ((amount>=20000)&&(amount<50000)) {( "Silver customers", List((cid,amount)) )}
      else if ((amount>=5000) &&(amount<20000)) {( "Bronze customers", List((cid,amount)) )}
      else {( "Lowtime customers", List((cid,amount)) )}
      }.reduceByKey(_ ++ _)
all_customers.toDF("Customer Type","Customer_id & Bill amount").show

/*
+-----------------+-------------------------+
|    Customer Type|Customer_id & Bill amount|
+-----------------+-------------------------+
|Lowtime customers|     [[a7c105e27ad87e9...|
|    VIP customers|     [[daf15f1b940cc6a...|
| Silver customers|     [[159132ab31eb3c8...|
| Bronze customers|     [[4dc417fbc348bf3...|
+-----------------+-------------------------+
*/

val all_customers_count = all_customers.map { case (customer_type,list)=> (customer_type, list.length) }.sortBy(_._2)
all_customers_count.toDF("Customer Type","Customer count").show

/*
+-----------------+--------------+
|    Customer Type|Customer count|
+-----------------+--------------+
|    VIP customers|             4|
| Silver customers|            11|
| Bronze customers|            50|
|Lowtime customers|          9475|
+-----------------+--------------+
*/

val file = new java.io.PrintStream("BDA_Project\\customer_type.csv")
all_customers_count.collect.foreach ( file.println(_) )
file.close


// Monthly Trend Forecasting:
// -------------------------

val ePairRdd5 = ePairRdd.map{ case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,oStat,pWt,pLen,pHt,pWidth,cCity,cState,sId,sCity,sState,sInstal) =>
      val ds_ts = ts.split(" ")
      val month = ds_ts(0).split("-")
      (pId,qt,month(1).toInt) }.sortBy(_._3)

val all_months = ePairRdd5.map{ case (pId,qt,month) =>
      ( month, List((pId,qt)) ) }.reduceByKey(_ ++ _).sortBy(_._1)

val all_months_count = ePairRdd5.map{ case (pId,qt,month) =>
      ( month,qt) }.reduceByKey(_+_).sortBy(_._1)

val ePairRdd6 = ePairRdd.map{ case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,oStat,pWt,pLen,pHt,pWidth,cCity,cState,sId,sCity,sState,sInstal) =>
      val ds_ts = ts.split(" ")
      val month = ds_ts(0).split("-")
      (month(1).toInt,cid) }.reduceByKey(_++_).sortBy(_._1)

val monthly_customers_count = ePairRdd6.map { case (month,cid)=> (month,cid.length) }.sortBy(_._1)

val monthly_price = ePairRdd.map{ case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,oStat,pWt,pLen,pHt,pWidth,cCity,cState,sId,sCity,sState,sInstal) =>
      val ds_ts = ts.split(" ")
      val month = ds_ts(0).split("-")
      (month(1).toInt,sp) }.reduceByKey(_+_).sortBy(_._1)

// Joining 3 RDDs
val monthly_analysis_joint = (all_months_count.join(monthly_customers_count)).join(monthly_price).sortBy(_._1)
monthly_analysis_joint.toDF.show

val monthly_analysis = monthly_analysis_joint.map{ case(month,q_c_p)=>
      val price = q_c_p._2
      val q_c = q_c_p._1
      val qt = q_c._1
      val customer_count = q_c._2
      (month,qt,customer_count,price) }
monthly_analysis.toDF("Month","Quantity","Customer count","Price").show

/*
+-----+--------+--------------+---------+
|Month|Quantity|Customer count|    Price|
+-----+--------+--------------+---------+
|    1|       5|           160|705.42004|
|    2|      29|           608|  6474.93|
|    3|     121|          3616|25786.564|
|    4|    2353|         56480| 354047.6|
|    5|    3478|         87616| 579959.3|
|    6|    3145|         79104|492078.03|
|    7|    2901|         75936| 474321.5|
|    8|    2654|         71584|387912.56|
|    9|       1|            32|   317.85|
|   10|       3|            96|285.03998|
|   11|       1|            32|   133.76|
|   12|       4|           128|   197.36|
+-----+--------+--------------+---------+
*/

val file = new java.io.PrintStream("BDA_Project\\monthly_analysis.csv")
monthly_analysis.collect.foreach ( file.println(_) )
file.close

// Monthly Profit Analysis:
// ------------------------

val pRdd=ePairRdd.map{case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,oStat,pWt,pLen,pHt,pWidth,cCity,cState,sId,sCity,sState,sInstal)=>
       val t1=ts.split(" ")(0).split("-")(1)
       (t1,(List(sp-cp),List(sp)))}.reduceByKey{case(a,b)=>(a._1++b._1,a._2++b._2)}.map{case(a,(b,c))=>
       val ps=b.sum
       val ss=c.sum
       val n=b.length
      (a,ps,ps/n,ps*100.0/ss)}.sortBy(x=>x._1)

pRdd.toDF("Month","Net Profit","Avg Profit","Profit Percentage").show
/*
+-----+----------+----------+-------------------+
|Month|Net Profit|Avg Profit|  Profit Percentage|
+-----+----------+----------+-------------------+
|   01|  89.72001| 17.944002| 12.718665621005606|
|   02|   3902.72| 205.40631| 60.274317479141494|
|   03| -838.5501|-7.4207973| -3.251887669594775|
|   04|  138351.0|  78.38583| 39.076966910684106|
|   05| 206943.22| 75.581894| 35.682460739331795|
|   06| 178368.52|  72.15555|  36.24804662730084|
|   07| 152446.88| 64.242256| 32.139984148411976|
|   08|109504.805| 48.951633| 28.229238020440647|
|   09| 17.860016| 17.860016|  5.619007558969206|
|   10|   -163.96|-54.653336|-57.521758122693434|
|   11| 13.859993| 13.859993| 10.361837283305736|
|   12|     35.39|    8.8475| 17.931698054419353|
+-----+----------+----------+-------------------+
*/

val file=new java.io.PrintStream("BDA_Project\\Monthly_Profit.csv")
pRdd.collect.foreach{x=> file.println(x._1+","+x._2+","+x._3+","+x._4)}
file.close

// Hourly sales analysis:
// ----------------------

val TRdd=ePairRdd.map{case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,oStat,pWt,pLen,pHt,pWidth,cCity,cState,sId,sCity,sState,sInstal)=>
      val t1=ts.split(" ")(1).split(":")(0)
      (t1,(List(qt),List(sp)))}.reduceByKey{case(a,b)=>(a._1++b._1,a._2++b._2)}.map{case(a,(b,c))=>(a,b.sum,c.sum/c.length)}.sortBy(_._1)

TRdd.toDF("Hour","Quantity","Avg Price").show
/*
+---+----+---------+
| _1|  _2|       _3|
+---+----+---------+
| 00| 380|188.42192|
| 01| 157|135.26366|
| 02|  71|141.61308|
| 03|  46| 144.3241|
| 04|  55|141.85236|
| 05|  17| 83.02766|
| 06|  91|150.37776|
| 07| 185|217.24957|
| 08| 478|171.79517|
| 09| 778| 207.7129|
| 10| 908| 186.6949|
| 11| 943| 165.2758|
| 12| 835|198.04971|
| 13|1003| 203.4392|
| 14|1060|272.49692|
| 15| 842|195.80493|
| 16|1161|199.62231|
| 17| 833|181.16876|
| 18| 926|221.36327|
| 19| 858|216.57292|
+---+----+---------+
only showing top 20 rows
*/

val file = new java.io.PrintStream("BDA_Project\\Hourly_analysis.csv")
TRdd.sortBy(x=>x._1).collect.foreach{x=> file.println(x._1+","+x._2+","+x._3)}
file.close


// Product Wise Analysis:
// ----------------------

val prodRdd=ePairRdd.map{case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,oStat,pWt,pLen,pHt,pWidth,cCity,cState,sId,sCity,sState,sInstal)=>
      (pId,(List(rating),List(sp)))}.reduceByKey{case(a,b)=>(a._1++b._1,a._2++b._2)}.map{case(a,(b,c))=>(a,b.sum*1.0/b.length,c.sum/c.length,c.length)}

// Total number of unique products sold:
prodRdd.count
//res28: Long = 6068

prodRdd.take(10).foreach(println)
/*
(2196812e80b905c7071389c10ad035d3,5.0,141.97,1)
(e24f73b7631ee3fbb2ab700a9acaa258,4.0,136.6,2)
(12fc9ab82dd45f3824881d94f79edb38,4.5,21.84,2)
(486cebbfac430b06cf0aa980b89a3543,1.0,232.88,1)
(04c31b0da644f1782603183f8fba274c,5.0,79.23,1)
(9680a78df870a1cb6ae00cb3436fe06a,4.0,108.63,1)
(f34152d311875e9da480bd51f495155d,5.0,46.61,1)
(b914a18d4547eff292fedeccc82faec6,1.0,767.33,1)
(af407c297ba8ec15891e2d95193203c2,5.0,144.46,1)
(192d363be840c4c307c77a1880c1d8d6,5.0,66.71,1)
*/


val pSorted=prodRdd.sortBy( x => (x._4), ascending = true).collect.toList

// Top-10 Most Sold products:
pSorted.reverse.take(10).foreach(println)
/*
(53b36df67ebb7c41585e8d54d6772e08,3.9859154929577465,126.67379,71)
(3fbc0ef745950c7932d5f2a446189725,3.3666666666666667,111.7982,60)
(19c91ef95d509ea33eda93495c4d3481,4.033898305084746,156.36154,59)
(422879e10f46682990de24d770e7f83d,3.875,151.90225,48)
(a92930c327948861c015c919a0bcb4a8,3.2954545454545454,88.88952,44)
(3dd2a17168ec895c781a9191c1e95ad7,3.9069767441860463,292.0474,43)
(d1c427060a0f73f6b889a5c7c61f2ac4,4.2439024390243905,173.98273,41)
(a62e25e09e05e6faf31d90c6ec1aa3d1,3.675,300.078,40)
(aca2eb7d00ea1a7b8ebd4e68314663af,3.871794871794872,114.70897,39)
(89b121bee266dcd25688a1ba72eefb61,1.7222222222222223,100.56999,36)
*/

// Least-10 sold products:
pSorted.take(10).foreach(println)
/*
(2196812e80b905c7071389c10ad035d3,5.0,141.97,1)
(486cebbfac430b06cf0aa980b89a3543,1.0,232.88,1)
(04c31b0da644f1782603183f8fba274c,5.0,79.23,1)
(9680a78df870a1cb6ae00cb3436fe06a,4.0,108.63,1)
(f34152d311875e9da480bd51f495155d,5.0,46.61,1)
(b914a18d4547eff292fedeccc82faec6,1.0,767.33,1)
(af407c297ba8ec15891e2d95193203c2,5.0,144.46,1)
(192d363be840c4c307c77a1880c1d8d6,5.0,66.71,1)
(b05fae603a3a28a977633c139cece058,4.0,316.4,1)
(ee08e2b1f2f38ce754c804780c6a048c,5.0,51.06,1)
*/


// Product Category Analysis:
// --------------------------

val pCatRdd=ePairRdd.map{case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,oStat,pWt,pLen,pHt,pWidth,cCity,cState,sId,sCity,sState,sInstal)=>
      (pCat,(List(rating),List(sp)))}.reduceByKey{case(a,b)=>(a._1++b._1,a._2++b._2)}.map{case(a,(b,c))=>(a,b.sum*1.0/b.length,c.sum/c.length,c.length)}

//Total number of unique product categories sold
pCatRdd.count
//res23: Long = 66

pCatRdd.take(10).foreach(println)
/*
(party_supplies,3.1666666666666665,99.714165,12)
(dvds_blu_ray,2.0,87.973335,3)
(agro_industry_and_commerce,3.6,394.45065,15)
(cds_dvds_musicals,5.0,117.58,1)
(computers,3.96,1895.7657,25)
(books_general_interest,4.085106382978723,113.190865,47)
(bed_bath_table,3.5972558514931396,158.00053,1239)
(telephony,3.719730941704036,144.22685,446)
(fashion_childrens_clothes,5.0,97.48,1)
(diapers_and_hygiene,3.5,74.175,4)
*/

val cSorted=pCatRdd.sortBy( x => (x._4), ascending = true).collect.toList

//Top 10 categories sold
cSorted.reverse.take(10).foreach(println)
/*
(bed_bath_table,3.5972558514931396,158.00053,1239)
(health_beauty,3.8967052537845057,179.26001,1123)
(housewares,3.6645569620253164,184.62833,948)
(watches_gifts,3.685774946921444,220.68384,942)
(sports_leisure,3.9197860962566846,158.82326,748)
(furniture_decor,3.590659340659341,199.38974,728)
(computers_accessories,3.654879773691655,188.66545,707)
(auto,3.810313075506446,190.96826,543)
(telephony,3.719730941704036,144.22685,446)
(garden_tools,4.0625,195.06085,384)
*/

//Least 10 categories sold
cSorted.take(10).foreach(println)
/*
(cds_dvds_musicals,5.0,117.58,1)
(fashion_childrens_clothes,5.0,97.48,1)
(flowers,3.0,35.72,1)
(tablets_printing_image,5.0,58.19,1)
(dvds_blu_ray,2.0,87.973335,3)
(diapers_and_hygiene,3.5,74.175,4)
(books_imported,3.75,221.2775,4)
(home_comfort_2,2.0,52.300003,4)
(fashion_underwear_beach,4.571428571428571,88.44857,7)
(arts_and_craftmanship,3.0,111.47876,8)
*/


//Ratings given
val ratingRdd=ePairRdd.map{case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,oStat,pWt,pLen,pHt,pWidth,cCity,cState,sId,sCity,sState,sInstal)=>
      (rating,1)}.reduceByKey(_+_)
//ratingRdd: org.apache.spark.rdd.RDD[(Int, Int)] = ShuffledRDD[98] at reduceByKey at <console>:26

ratingRdd.sortBy(x=>x._1).toDF.show
/*
+---+----+
| _1|  _2|
+---+----+
|  1|2244|
|  2| 609|
|  3| 945|
|  4|1743|
|  5|6190|
+---+----+
*/

// Payment prefernce:
// ------------------

val ePairRdd7 = ePairRdd.map{ case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,
       oStat,pWt,pLen,pHt,pWidth,cCity,
       cState,sId,sCity,sState,sInstal) => (sp_type,1) }.reduceByKey(_+_).sortBy(_._2,false)

val ePairRdd8 = ePairRdd.map{ case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,
       oStat,pWt,pLen,pHt,pWidth,cCity,
       cState,sId,sCity,sState,sInstal) => (sp_type,sp*qt) }.reduceByKey(_+_).sortBy(_._2,false)

// Joining 2 RDDs
val ePairRdd10 = ePairRdd7.join(ePairRdd8).map{ case(sp_type,q_p)=>
      val qt = q_p._1
      val price = q_p._2
      (sp_type,qt,price) }.sortBy(_._2,false)

// Most commonly used payment types:
val ePairRdd11 = ePairRdd10.map{ case(sp_type,qt,price)=>
      val avg_price = price/qt
      (sp_type,qt,price,avg_price) }
ePairRdd11.toDF("Payment Type","Count","Total Amount spent","Avg Amount spent per order").show 

/*
+------------+-----+------------------+--------------------------+
|Payment Type|Count|Total Amount spent|Avg Amount spent per order|
+------------+-----+------------------+--------------------------+
| credit_card| 8786|         2526096.8|                 287.51385|
|      boleto| 2118|         925155.56|                  436.8062|
|     voucher|  515|          42341.64|                  82.21678|
|  debit_card|  312|           63289.8|                 202.85193|
+------------+-----+------------------+--------------------------+
*/

val file = new java.io.PrintStream("BDA_Project\\sp_type.csv")
ePairRdd11.foreach ( file.println(_) )
file.close

// Count of Orders With each No. of Payment Installments:
val ePairRdd12 = ePairRdd.map{ case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,
       oStat,pWt,pLen,pHt,pWidth,cCity,
       cState,sId,sCity,sState,sInstal) => (sInstal,1) }.reduceByKey(_+_).sortBy(_._1)
ePairRdd12.toDF("No. of Payment Installments","Count for each installment").show

/*
+---------------------------+--------------------------+
|No. of Payment Installments|Count for each installment|
+---------------------------+--------------------------+
|                          1|                      5669|
|                          2|                      1345|
|                          3|                      1150|
|                          4|                       812|
|                          5|                       601|
|                          6|                       503|
|                          7|                       176|
|                          8|                       667|
|                          9|                        60|
|                         10|                       697|
|                         11|                         1|
|                         12|                        18|
|                         13|                         3|
|                         14|                         2|
|                         15|                        12|
|                         16|                         4|
|                         17|                         1|
|                         18|                         8|
|                         20|                         2|
+---------------------------+--------------------------+
*/

val file = new java.io.PrintStream("BDA_Project\\no_of_installment.csv")
ePairRdd12.collect.foreach ( file.println(_) )
file.close


// Potential Customer Location: - Statewise
// ----------------------------

val ePairRdd13 = ePairRdd.map{ case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,
       oStat,pWt,pLen,pHt,pWidth,cCity,
       cState,sId,sCity,sState,sInstal) => (cState,1) }.reduceByKey(_+_).sortBy(_._2,false)

val ePairRdd14 = ePairRdd.map{ case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,
       oStat,pWt,pLen,pHt,pWidth,cCity,
       cState,sId,sCity,sState,sInstal) => (cState,sp*qt) }.reduceByKey(_+_).sortBy(_._2,false)

// Joining 2 RDDs
val ePairRdd15 = ePairRdd13.join(ePairRdd14).map{ case(cState,q_p)=>
      val qt = q_p._1
      val price = q_p._2
      (cState,qt,price) }.sortBy(_._2,false)

val ePairRdd16 = ePairRdd15.map{ case(cState,qt,price)=>
      val avg_price = price/qt
      (cState,qt,price,avg_price) }.sortBy(_._3,false)
ePairRdd16.toDF("Customer State","Count","Total Amount spent","Avg Amount spent per order").show 

/*
+--------------+-----+------------------+--------------------------+
|Customer State|Count|Total Amount spent|Avg Amount spent per order|
+--------------+-----+------------------+--------------------------+
|            SP| 5103|         1246241.2|                 244.21738|
|            RJ| 1501|          435567.5|                 290.18488|
|            MG| 1264|          359334.5|                 284.28363|
|            SC|  376|         231889.25|                 616.72675|
|            PR|  509|         167442.97|                 328.96457|
|            RS|  520|          161983.2|                 311.50616|
|            ES|  248|          133218.7|                  537.1722|
|            BA|  459|          128706.6|                 280.40652|
|            MT|  135|        109549.266|                   811.476|
|            GO|  228|          81480.17|                 357.36917|
|            MA|   67|          79351.27|                 1184.3474|
|            DF|  258|         75682.586|                 293.34335|
|            PE|  235|          65884.42|                 280.35925|
|            CE|  168|          61359.96|                 365.23785|
|            PA|  145|         55289.375|                 381.30603|
|            PB|   79|         34791.336|                 440.39667|
|            MS|   94|         28896.066|                 307.40497|
|            RN|   78|          28002.26|                 359.00333|
|            PI|   68|          18896.61|                 277.89133|
|            TO|   38|           14490.5|                 381.32895|
+--------------+-----+------------------+--------------------------+
*/

val file = new java.io.PrintStream("BDA_Project\\statewise_sales.csv")
ePairRdd16.collect.foreach ( file.println(_) )
file.close

// Potential Customer Location: - Citywise
// ----------------------------

val ePairRdd17 = ePairRdd.map{ case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,
       oStat,pWt,pLen,pHt,pWidth,cCity,
       cState,sId,sCity,sState,sInstal) => (cCity,1) }.reduceByKey(_+_).sortBy(_._2,false)

val ePairRdd18 = ePairRdd.map{ case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,
       oStat,pWt,pLen,pHt,pWidth,cCity,
       cState,sId,sCity,sState,sInstal) => (cCity,sp*qt) }.reduceByKey(_+_).sortBy(_._2,false)

// Joining 2 RDDs
val ePairRdd19 = ePairRdd17.join(ePairRdd18).map{ case(cCity,q_p)=>
      val qt = q_p._1
      val price = q_p._2
      (cCity,qt,price) }.sortBy(_._2,false)

val ePairRdd20 = ePairRdd19.map{ case(cCity,qt,price)=>
      val avg_price = price/qt
      (cCity,qt,price,avg_price) }.sortBy(_._3,false)
ePairRdd20.toDF("Customer City","Count","Total Amount spent","Avg Amount spent per order").show 

/*
+--------------------+-----+------------------+--------------------------+
|       Customer City|Count|Total Amount spent|Avg Amount spent per order|
+--------------------+-----+------------------+--------------------------+
|           sao paulo| 1917|         475675.66|                 248.13545|
|      rio de janeiro|  788|         253107.45|                 321.20236|
|         celso ramos|   12|          97263.66|                 8105.3047|
|          vila velha|   56|          85253.87|                 1522.3905|
|            brasilia|  258|         75682.586|                 293.34335|
|      belo horizonte|  312|          73878.53|                 236.79016|
|            salvador|  181|          72391.22|                 399.95148|
|            sao luis|   30|         70746.414|                 2358.2139|
|campo novo do par...|    7|          68208.54|                  9744.077|
|            valinhos|   41|         50681.918|                 1236.1443|
|           fortaleza|   87|         41759.633|                  479.9958|
|            curitiba|  137|          39034.86|                   284.926|
|           guarulhos|  153|         36826.586|                 240.69664|
|           rio verde|   19|          36516.27|                 1921.9089|
|            campinas|  161|          34916.57|                 216.87311|
|        porto alegre|  128|          33106.09|                 258.64133|
|              santos|   96|         31323.637|                 326.28787|
|             colombo|   13|          30087.07|                 2314.3901|
|         joao pessoa|   37|           24550.9|                 663.53784|
|           araucaria|   12|          23411.39|                 1950.9492|
+--------------------+-----+------------------+--------------------------+
*/

val file = new java.io.PrintStream("BDA_Project\\city_wise_sales.csv")
ePairRdd20.collect.foreach ( file.println(_) )
file.close

// Seller Ranking:
// ---------------

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

// Logistics Based Optimization Insights:
// --------------------------------------

// Which city buys more heavy wieght products?

val city_weight = ePairRdd.map{ case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,oStat,pWt,pLen,pHt,pWidth,cCity,cState,sId,sCity,sState,sInstal) => 
       var no_of_w = 0
       if ((pWt<3000)&&(pWt>=1000)) {no_of_w += 1}
       else if ((pWt<1000)&&(pWt>=200)) {no_of_w += 2}
       else if (pWt<200) {no_of_w += 3}
       else {no_of_w += 4}
       (cCity,List(no_of_w)) }.reduceByKey(_++_)

val city_weight_2 = city_weight.map{ case(cCity,weight_list) => 
       val collection = weight_list
       val new_collection = collection.map(x => (x,1))
       val res = new_collection.reduce( (a,b) => ( a._1 + b._1,a._2 + b._2 ) )
       val avg_weight = (res._1/res._2).toFloat
       (cCity,avg_weight) }
city_weight_2.toDF("City","Weight Category").show

val city_weight_label = city_weight_2.map{ case(cCity,weight) => 
       var label = "None"
       if (weight==1) label="Heavy"
       else if (weight==2) label="Slightly Heavy"
       else if (weight==2) label="Medium"
       else label="Light"
       (cCity,label) }

val city_weight_list = city_weight_label.map{ case(cCity,label) => 
       (label,List(cCity))}.reduceByKey(_++_).map{ case(label,cCity_list) => 
       (label,cCity_list,cCity_list.size) }
city_weight_list.toDF("Weight Category","City","City Count").show

/*
+---------------+--------------------+----------+
|Weight Category|                City|City Count|
+---------------+--------------------+----------+
|          Light|[sao gabriel da p...|       323|
|          Heavy|[bandeirantes, ca...|       389|
| Slightly Heavy|[sao francisco de...|       873|
+---------------+--------------------+----------+
*/

val file = new java.io.PrintStream("BDA_Project\\city_weight_based.csv")
city_weight_list.collect.foreach ( file.println(_) )
file.close

// How many proucts are domestic sales(Sold within seller city itself)?

val sales_loc = ePairRdd.map{ case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,oStat,pWt,pLen,pHt,pWidth,cCity,cState,sId,sCity,sState,sInstal) => 
       var count = 0
       if (cCity == sCity) count += 1
       (cCity,sCity,count) }.filter(x => (x._1 == x._2)).map{ x=> (x._1,x._3) }.reduceByKey(_+_)
sales_loc.toDF("City","Count").show

/*
+--------------------+---+
|                  _1| _2|
+--------------------+---+
|            salvador|  1|
|           fortaleza|  1|
|            sao luis|  2|
|             goiania|  1|
|         nova iguacu|  3|
|            contagem|  1|
|            campinas|  9|
|        porto alegre|  1|
|           sao paulo|542|
|           joinville|  1|
|           guarulhos| 10|
|      belo horizonte| 10|
|            sao jose|  1|
|              santos|  1|
| sao jose dos campos|  2|
|         santo andre|  2|
|            curitiba| 13|
|             limeira|  1|
|sao bernardo do c...|  1|
|              sumare|  1|
+--------------------+---+
*/

val file = new java.io.PrintStream("BDA_Project\\domestic_sales.csv")
sales_loc.collect.foreach ( file.println(_) )
file.close

// Domestic vs Foreign sales:
val domestic_sales_count = sales_loc.map{ x => (x._2)}.sum.toInt
val foriegn_sales_count = (ePairRdd.count - domestic_sales_count).toInt
val location_count = sc.parallelize( Array((domestic_sales_count,foriegn_sales_count)) )
location_count.toDF("Domestic Sales","Foreign Sales").show

/*
+--------------+-------------+
|Domestic Sales|Foreign Sales|
+--------------+-------------+
|           650|        11081|
+--------------+-------------+
*/

val file = new java.io.PrintStream("BDA_Project\\location_sales.csv")
location_count.collect.foreach ( file.println(_) )
file.close

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


/*
Contents of Project:
--------------------

1. Customer Segmentation (Categorizing customers based on their spendings)
   [Bar-graph]

2. Monthly Trend Forecasting (Visualising the monthly trend of sales)
   [Bar-graph]

3. Which hour has more no. of sales? (Also avg qt & price for each hour)
   [Timeseries-Plot]

4. Do average (instead of sum) for monthly trend analysis
   [Bar-graph]

5. Which category product has sold more? 
   & Which category product has more rating (Category wise avg price,rating & Product wise avg price,rating)
   and
   Which product has sold more? 
   & Top 10 highest & least product rating?
   and
   Order Count for each rating
   [Bar-graph]

6. What are the most commonly used sp types? (avg price for each sp type)
   &
   Count of Orders With each No. of Payment Installments
   [Pie-Chart]

7. Where do most customers come from? (State wise & city wise avg sales) 
   [Pie-chart]

8. Which seller sold more? & Which seller got more rating? (avg price & rating for each seller)
   [Bar-graph]


Logistics Based:
----------------

9. Which city buys heavy weight products and low weight products? (City wise avg weight)
   [Pie-chart]

10. How much products sold within seller state? (if (seller state == customer state) then count++)
   & How much products sold outside his state?
   [Bar-graph]

Machine Learning Model:
-----------------------

11. Predicting future sales (ML - Linear regression)

Visualization:
--------------

Python Plots

*/
