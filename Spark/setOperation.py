#SET OPERATION
orders = sc.textFile("/user/maria_dev/data/retail_db/orders/")
orderItems = sc.textFile("/user/maria_dev/data/retail_db/order_items")

orders201312 = orders.filter(lambda o: \
o.split(",")[1][:7] == "2013-12").map(lambda o: (int(o.split(",")[0]),o))

orders201401 = orders.filter(lambda o: \
o.split(",")[1][:7] == "2014-01").map(lambda o: (int(o.split(",")[0]),o))

orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]),oi))

ordersItems201312 = orders201312.join(orderItemsMap).map(lambda oi: oi[1][1]) //orderItems
ordersItems201401 = orders201401.join(orderItemsMap).map(lambda oi: oi[1][1]) //orderItems
------------------------------------------------------
SET OPERATION - Union - Get products ids sold in 2013-12 & 2014-01
products201312 = ordersItems201312.map(lambda p: int(p.split(",")[2])) //product id
products201401 = ordersItems201401.map(lambda p: int(p.split(",")[2])) //product id
allproducts = products201312.union(products201401).distinct() //Union
commonproducts = products201312.intersection(products201401).distinct() //intersect
SET OPERATION - mins - Get products in 2013-12 but not in 2014-01
products201312only = products201312.subtract(products201401).distinct() //min
products201401only = products201401.subtract(products201312).distinct()
productsOnlyInOneMonth = products201312only.union(products201401only)
