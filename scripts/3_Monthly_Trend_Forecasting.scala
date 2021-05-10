// Monthly Trend Forecasting:
// -------------------------

:load 1_load_data.scala

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