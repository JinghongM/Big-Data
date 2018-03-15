#SAVE INTO FILE
orderItems = sc.textFile("/user/maria_dev/data/retail_db/order_items")
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]),float(oi.split(",")[4]))).map(lambda x:(x[0],round(x[1],2)))
revenuePerOrderId = orderItemsMap.reduceByKey(add).map(lambda x: str(x[0]) + "\t" + str(x[1]))
revenuePerOrderId.saveAsTextFile("/user/maria_dev/data/")
---------------------------------------------------
#Compress
revenuePerOrderId.saveAsTextFile("/user/maria_dev/data/revenue_per_orderidCompressor", compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
----------------------------------------------------
#Save as json
orderItems = sc.textFile("/user/maria_dev/data/retail_db/order_items")
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]),float(oi.split(",")[4])))
revenuePerOrderId = orderItemsMap.reduceByKey(add).map(lambda x:(x[0],round(x[1],2)))
revenuePerOrderIdDF = revenuePerOrderId.toDF(schema=["order_id","order_revenue"])
revenuePerOrderIdDF.save("/user/maria_dev/data/revenue_per_orderid_Json","json")
revenuePerOrderIdDF.write.json("/user/maria_dev/data/revenue_per_orderid_Json")

sqlContext.read.json("/user/maria_dev/data/revenue_per_orderid_Json").show()