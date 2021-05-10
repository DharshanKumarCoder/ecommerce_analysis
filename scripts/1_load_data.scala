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
+--------------------+--------------------+---+-----+------+----------------+------+-------------+--------------------+-----------+---------+---+----+---+------+--------------------+------+--------------------+--------------------+------+-------+
|                 oid|                 cid| qt|   cp|    sp|              ts|rating|         pCat|                 pId|    sp_type|    oStat|pWt|pLen|pHt|pWidth|               cCity|cState|                 sId|               sCity|sState|sInstal|
+--------------------+--------------------+---+-----+------+----------------+------+-------------+--------------------+-----------+---------+---+----+---+------+--------------------+------+--------------------+--------------------+------+-------+
|d1ff908b4e21d4fff...|190508c583e9da289...|  1|207.9|238.61|04-08-2018 21:57|     5|health_beauty|08462528607b71ea6...|credit_card|delivered|650|  16| 10|    11|sao goncalo do am...|    RN|ccc4bbb5f32a6ab2b...|            curitiba|    PR|      8|
|093c11a6b12993f96...|77a6136f49bac0c33...|  1|364.0|797.38|13-06-2018 08:52|     5|health_beauty|6cdd53843498f9289...|credit_card|delivered|900|  25| 12|    38|jaboatao dos guar...|    PE|ccc4bbb5f32a6ab2b...|            curitiba|    PR|      6|
|093c11a6b12993f96...|77a6136f49bac0c33...|  2|364.0|797.38|13-06-2018 08:52|     5|health_beauty|6cdd53843498f9289...|credit_card|delivered|900|  25| 12|    38|jaboatao dos guar...|    PE|ccc4bbb5f32a6ab2b...|            curitiba|    PR|      6|
|d08bba6d656adbdc5...|2bbb7f863b68ba0f0...|  2|364.0|479.22|13-07-2018 20:40|     1|health_beauty|6cdd53843498f9289...|credit_card|delivered|900|  25| 12|    38|      rio de janeiro|    RJ|ccc4bbb5f32a6ab2b...|            curitiba|    PR|      2|
|8be26417e499e19c5...|08345c45f60f73773...|  1|364.0| 428.8|05-08-2018 23:13|     5|health_beauty|6cdd53843498f9289...|credit_card|delivered|900|  25| 12|    38|            toritama|    PE|ccc4bbb5f32a6ab2b...|            curitiba|    PR|      8|
|0d711a2c41f1b9365...|213904d01203d2e28...|  1|349.9|365.43|24-04-2018 15:27|     5|health_beauty|6cdd53843498f9289...|credit_card|delivered|900|  25| 12|    38|      rio de janeiro|    RJ|ccc4bbb5f32a6ab2b...|            curitiba|    PR|      1|
|d7bcd768f38943877...|bce98ac8bc5881286...|  1|349.9|398.46|08-05-2018 09:22|     5|health_beauty|6cdd53843498f9289...|credit_card|delivered|900|  25| 12|    38|              anadia|    AL|ccc4bbb5f32a6ab2b...|            curitiba|    PR|      8|
|91d9b7669f0960199...|c9341f47a1c2979e0...|  1|349.9|370.52|10-04-2018 14:36|     5|health_beauty|6cdd53843498f9289...|credit_card|delivered|900|  25| 12|    38|      rio de janeiro|    RJ|ccc4bbb5f32a6ab2b...|            curitiba|    PR|      8|
|48af7589aa4fbcb01...|e2f2b94effb9d9eab...|  1|364.0| 395.8|04-08-2018 17:09|     5|health_beauty|6cdd53843498f9289...|credit_card|delivered|900|  25| 12|    38|             aracaju|    SE|ccc4bbb5f32a6ab2b...|            curitiba|    PR|      6|
|209b30577b4c8a3f6...|f3cbdf0bb95fae3bb...|  1|349.9|381.86|04-04-2018 20:50|     5|health_beauty|6cdd53843498f9289...|credit_card|delivered|900|  25| 12|    38|             aracaju|    SE|ccc4bbb5f32a6ab2b...|            curitiba|    PR|      5|
|5262eaeb971616ffe...|35f335e770081fe4f...|  1| 28.9|  0.67|08-06-2018 18:51|     5|health_beauty|d1a57f4402dbfe2cb...|credit_card|delivered|650|  22| 12|    22|            linhares|    ES|2ff97219cb8622eaf...|               salto|    SP|      1|
|5262eaeb971616ffe...|35f335e770081fe4f...|  1| 28.9| 47.55|08-06-2018 18:51|     5|health_beauty|d1a57f4402dbfe2cb...|    voucher|delivered|650|  22| 12|    22|            linhares|    ES|2ff97219cb8622eaf...|               salto|    SP|      1|
|946f3e7b0d6366f44...|0e6e8c5b4485a66e6...|  1| 31.9| 97.31|09-08-2018 07:25|     3|health_beauty|d1a57f4402dbfe2cb...|credit_card|delivered|650|  22| 12|    22|             niteroi|    RJ|2ff97219cb8622eaf...|               salto|    SP|      8|
|1b0e1058a094f3339...|91e07c844b9c9cd8a...|  1| 29.9| 39.42|09-07-2018 19:51|     5|health_beauty|d1a57f4402dbfe2cb...|credit_card|delivered|650|  22| 12|    22|         hortolandia|    SP|2ff97219cb8622eaf...|               salto|    SP|      1|
|a684a880595e155b0...|6a43e91f4891c9b7b...|  1| 49.8| 68.03|10-06-2018 18:51|     1|health_beauty|8ddf5554f2617f632...|credit_card|delivered|400|  16|  2|    11|      ribeirao preto|    SP|2a1348e9addc1af5a...|      belo horizonte|    MG|      2|
|9d65f5b3f39a8bab1...|d73568e3ecaf64187...|  1| 84.9| 93.79|24-05-2018 23:50|     5|health_beauty|c46c87888b081cf42...|credit_card|delivered|450|  27| 11|    19|             cajamar|    SP|41e0fa5761c886a63...|             guaruja|    SP|      8|
|d0d2bc60ab96a5450...|23e8f9579ef5c6aef...|  1| 77.0| 90.88|30-06-2018 13:32|     5|health_beauty|719d571299707561c...|credit_card|delivered|434|  20| 16|    18|               imbau|    PR|b90e891671cffd955...|sao jose dos pinhais|    PR|      4|
|48e70325bb404002e...|95556cea2fc7ddfac...|  1|199.0|427.02|05-05-2018 08:28|     1|health_beauty|a6f2ba5bb6d6d36ac...|credit_card|delivered|450|  20| 15|    15|        sertanopolis|    PR|b90e891671cffd955...|sao jose dos pinhais|    PR|      3|
|48e70325bb404002e...|95556cea2fc7ddfac...|  2|199.0|427.02|05-05-2018 08:28|     1|health_beauty|a6f2ba5bb6d6d36ac...|credit_card|delivered|450|  20| 15|    15|        sertanopolis|    PR|b90e891671cffd955...|sao jose dos pinhais|    PR|      3|
|717b5830932c155c6...|acec81168c11c0d01...|  1|140.0|159.08|23-07-2018 12:50|     5|health_beauty|d4e19eb1e5c5d6194...|credit_card|delivered|300|  16| 20|    15|          mogi-guacu|    SP|b90e891671cffd955...|sao jose dos pinhais|    PR|      5|
+--------------------+--------------------+---+-----+------+----------------+------+-------------+--------------------+-----------+---------+---+----+---+------+--------------------+------+--------------------+--------------------+------+-------+
*/
