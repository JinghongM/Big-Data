#REDUCE BY KEY
#use rdd.map function and convert to a new RDD
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]),float(oi.split(",")[4])))

#use rdd.reduce function to aggregate by the First value in a tuple and do calculation
revenuePerOrder = orderItemsMap.reduceByKey(lambda curr,next: curr+next)
----------------------------------------------
orderItems = sc.textFile("/user/maria_dev/data/retail_db/order_items")
orderItemsMap = orderItems.map(lambda oi: \
(int(oi.split(",")[1]),float(oi.split(",")[4])))

from operator import add
revenuePerOrderId = orderItemsMap.reduceByKey(add)
revenuePerOrderId = orderItemsMap.reduceByKey(lambda x,y : x + y)

minSubtotalPerOrderId = orderItemsMap.reduceByKey(lambda x,y: x if x < y else y)

#Get order item details with minimum subtotal for each order_id
orderItemsMap = orderItems.map(lambda oi: \
	(int(oi.split(",")[1]),oi))
minSubtotalPerOrderId = orderItemsMap.reduceByKey(lambda x,y: \
	x if x.split(",")[4] < y.split(",")[4] else y )
