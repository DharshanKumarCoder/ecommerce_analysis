// Customer Segmentation:
// ----------------------

:load 1_load_data.scala

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