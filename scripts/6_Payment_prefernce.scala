// Payment prefernce:
// ------------------

:load 1_load_data.scala

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

val file = new java.io.PrintStream("BDA_Project\\payment_preference.csv")
ePairRdd11.sortBy(_._2,false).collect.foreach ( x => file.println(x._1+","+x._2+","+x._3+","+x._4) )
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


