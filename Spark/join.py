#JOIN
orders = sc.textFile("/user/maria_dev/data/retail_db/orders/")
#(order_id)(order_date)(order_customer_id)(status)'
orderMap = orders.map ( \
lambda record: (int(record.split(",")[0]),record.split(",")[3]))
		(order_id,status)
orderItems = sc.textFile("/user/maria_dev/data/retail_db/order_items")
#(order_item_id)(order_item_order_id)(order_item_product_id)(quantity)(order_item_subtotal)(product_price)'
orderItemsMap = orderItems.map ( \
lambda record: (int(record.split(",")[1]),float(record.split(",")[4])))
		(order_id,subtotal)
#Inner Join
ordersJoin = orderMap.join(orderItemsMap)
for i in ordersJoin.take(10):print(i)
#LeftOuterJoin
ordersLeftOuterJoin = orderMap.leftOuterJoin(orderItemsMap)
ordersLeftOuterFilter = ordersLeftOuterJoin.filter(lambda o:o[1][1] == None)
#RightOuterJoin
ordersRightOuterJoin = orderItemsMap.rightOuterJoin(orderMap)
ordersRightOuterFilter = ordersRightOuterJoin.filter(lambda o:o[1][0] == None)
