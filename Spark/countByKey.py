#COUNT BY KEY
orders = sc.textFile("/user/maria_dev/data/retail_db/orders/")
orderStatus = orders.map(lambda o: \
(o.split(",")[3],1))
countByStatus = orderStatus.countByKey()
