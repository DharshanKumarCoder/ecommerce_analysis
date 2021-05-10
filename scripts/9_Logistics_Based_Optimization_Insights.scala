// Logistics Based Optimization Insights:
// --------------------------------------

:load 1_load_data.scala

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