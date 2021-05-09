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
