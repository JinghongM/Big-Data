#FILTER
orders = sc.textFile("/user/maria_dev/data/retail_db/orders/part-00000")
ordersComplete = orders.filter(lambda order:order.split(",")[-1] == "COMPLETE")
ordersComplete = orders.filter(lambda order:order.split(",")[-1] == "COMPLETE" or order.split(",")[-1] == "CLOSED")
ordersComplete = orders.filter(lambda order: \
(order.split(",")[-1] in ["COMPLETE", "CLOSED"]) and order.split(",")[1][:7] =="2014-01"
)
