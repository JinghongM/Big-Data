#AGGREGATE BY KEY:
orderItems = sc.textFile("/user/maria_dev/data/retail_db/order_items")
orderItemsMap = orderItems.map(lambda oi: \
(int(oi.split(",")[1]),float(oi.split(",")[4])))
orderItemsAggregateByKey = orderItemsMap.aggregateByKey((0.0,0), \
lambda x,y: (x[0] + y, x[1] + 1 ),  \
lambda x,y: (x[0] + y[0],x[1] + y[1]))
First lambda: // x:(0.0,0) y: 199.99  ->   | x:(129.99,1)
Second lambda: x: :(129.99,1) y:(199.99,1)