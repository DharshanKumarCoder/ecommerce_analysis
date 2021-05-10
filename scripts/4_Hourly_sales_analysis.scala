// Hourly sales analysis:
// ----------------------

:load 1_load_data.scala

val TRdd=ePairRdd.map{case(oid,cid,qt,cp,sp,ts,rating,pCat,pId,sp_type,oStat,pWt,pLen,pHt,pWidth,cCity,cState,sId,sCity,sState,sInstal)=>
      val t1=ts.split(" ")(1).split(":")(0)
      (t1,(List(qt),List(sp)))}.reduceByKey{case(a,b)=>(a._1++b._1,a._2++b._2)}.map{case(a,(b,c))=>(a,b.sum,c.sum/c.length)}.sortBy(_._1)

TRdd.toDF("Hour","Quantity","Avg Price").show
/*
+----+--------+---------+
|Hour|Quantity|Avg Price|
+----+--------+---------+
|  00|     380|188.42192|
|  01|     157|135.26366|
|  02|      71|141.61308|
|  03|      46| 144.3241|
|  04|      55|141.85236|
|  05|      17| 83.02766|
|  06|      91|150.37776|
|  07|     185|217.24957|
|  08|     478|171.79517|
|  09|     778| 207.7129|
|  10|     908| 186.6949|
|  11|     943| 165.2758|
|  12|     835|198.04971|
|  13|    1003| 203.4392|
|  14|    1060|272.49692|
|  15|     842|195.80493|
|  16|    1161|199.62231|
|  17|     833|181.16876|
|  18|     926|221.36327|
|  19|     858|216.57292|
+----+--------+---------+
*/

val file = new java.io.PrintStream("BDA_Project\\Hourly_analysis.csv")
TRdd.sortBy(x=>x._1).collect.foreach{x=> file.println(x._1+","+x._2+","+x._3)}
file.close