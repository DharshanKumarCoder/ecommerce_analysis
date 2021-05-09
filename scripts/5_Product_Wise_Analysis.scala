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