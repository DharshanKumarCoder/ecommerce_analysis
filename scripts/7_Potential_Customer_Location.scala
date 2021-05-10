// Potential Customer Location: - Statewise
// ----------------------------

:load 1_load_data.scala

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
ePairRdd16.collect.foreach{x=> file.println(x._1+","+x._2+","+x._3+","+x._4)}
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
ePairRdd20.collect.foreach{x=> file.println(x._1+","+x._2+","+x._3+","+x._4)}
file.close