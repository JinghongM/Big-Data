#AGGREGATION
orderItems = sc.textFile("/user/maria_dev/data/retail_db/order_items")
orderItemsFiltered = orderItems.filter( \
lambda oi: int(oi.split(",")[1]) == 2)
orderItemsSubtotals = orderItemsFiltered.map( \
lambda oi: float(oi.split(",")[4]))
from operator import add
orderItemsSubtotals.reduce(add) OR orderItemsSubtotals.reduce(lambda x,y: x + y)
#Get order items details which have minimum order_item_subtotal for given order_id
orderItems = sc.textFile("/user/maria_dev/data/retail_db/order_items")
orderItemsFiltered = orderItems.filter(lambda oi: int(oi.split(",")[1]) == 2)
orderItemsReduce = orderItemsFiltered.reduce(lambda x,y: \
x if (float(x.split(",")[4])) < float(y.split(",")[4]) else y)

