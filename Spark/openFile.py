#read data as RDD
orderItems = sc.textFile("/user/maria_dev/data/retail_db/order_items/part-00000") OR  orderItems = sc.textFile("file:///home/maria_dev/cca-175/retail_db/order_items/part-00000")

#Read Json File
sqlContext.read.json("./File/You/Want")

#open a file and create a RDD
productsRaw = open("./retail_db/products/part-00000").read().splitlines()
productRDD = sc.parallelize(productsRaw

#convert to list
OrderItemsList = orderItems.collect()

for i in revenuePerOrder.take(10):print(i)

//open a file and create a RDD
productsRaw = open("./retail_db/products/part-00000").read().splitlines()
productRDD = sc.parallelize(productsRaw)