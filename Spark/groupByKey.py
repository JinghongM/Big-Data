#GROUP BY KEY
orderItems = sc.textFile("/user/maria_dev/data/retail_db/order_items")
orderItemsMap = orderItems.map(lambda oi: \
(int(oi.split(",")[1]),float(oi.split(",")[4])))
orderItemsGroupByOrderId = orderItemsMap.groupByKey()
-------------------------------------------
#GROUP BY KEY: get order items sorted by order_item_subtotal for each order ID

orderItems = sc.textFile("/user/maria_dev/data/retail_db/order_items")
orderItemsMap = orderItems.map(lambda oi: \
(int(oi.split(",")[1]),oi))
orderItemsGroupByID = orderItemsMap.groupByKey()
orderItemsSortedBySubPerOrder = orderItemsGroupByID.flatMap(lambda ois: \
sorted(ois[1],key = lambda k: float(k.split(",")[4]),reverse=True))
------------------------------------------
#GROUP BY KEY
products = sc.textFile("/user/maria_dev/data/retail_db/products")
productsFiltered = products.filter(lambda p: p.split(",")[4] != "")
productsMap = productsFiltered.map(lambda p: \
(int(p.split(",")[1]),p))
productsGroupByCategoryId = productsMap.groupByKey()
t = productsGroupByCategoryId.first()
sorted(t[1],key=lambda k: float(k.split(",")[4]))
topNProductsByCategory = productsGroupByCategoryId.flatMap(lambda p:sorted(p[1],key=lambda k: float(k.split(",")[4]),reverse=True)[:3])
