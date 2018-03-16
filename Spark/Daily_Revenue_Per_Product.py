# Get daily revenue by product considering completed and closed orders.
# Data sorted by ascending order by date, descending order by revenue computed for each product for each day.
# Deliminted by "," - order_date,daily_revenue_per_product,product_name

# departments: department_id department_name

# categories: categories_id, category_department_id, company_name

# product: product_id,category_id,product_name,product_description,product_price,product_image
# order_items:order_items_id,order_id,product_id,quantity,subtotal,product_price

# orders:order_id,order_date,order_customer_id,order_status


orders = sc.textFile("/user/maria_dev/data/retail_db/orders")
ordersFilter = orders.filter(lambda x: \
(x.split(",")[3] == "COMPLETE" or x.split(",")[3] == "CLOSED"))
ordersMap = ordersFilter.map(lambda x: (int(x.split(",")[0]),x.split(",")[1]))

orderItems = sc.textFile("/user/maria_dev/data/retail_db/order_items")
orderItemsMap = orderItems.map(lambda x: \
(int(x.split(",")[1]),(int(x.split(",")[2]),float(x.split(",")[4]))))

orderItemsJoin = orderItemsMap.join(ordersMap)
#(order_id,((product_id,subtotal),date)) -> ((date,product_id),subtotal)

orderItemsJoinMap = orderItemsJoin.map(lambda x: ((x[1][1],x[1][0][0]),x[1][0][1]))
from operator import add 
orderItemsReduceByKeys = orderItemsJoinMap.reduceByKey(add)
orderItemsID = orderItemsReduceByKeys.map(lambda x: (x[0][1],(x[0][0],x[1])))

products = sc.textFile("file:///home/maria_dev/cca-175/retail_db/products")
productsMap = products.map(lambda x: (int(x.split(",")[0]),x.split(",")[2]))
productsJoin = productsMap.join(orderItemsID)
productsMap = productsJoin.map(lambda x:((x[1][1][0],-x[1][1][1]),x[1][0]))
productsSort = productsMap.sortByKey().map(lambda x: (x[0][0],-x[0][1],x[1]))

productsSort.saveAsTextFile("/user/maria_dev/daily_revenue_txt_python")
productsSort.saveAsTextFile("file:///home/maria_dev/daily_revenue_txt_python")
productsSortDF = productsSort.coalesce(2).toDF(schema=["order_date","revenue_per_product","product_name"])
productsSortDF.save("/user/maria_dev/daily_revenue_avro_python","com.databricks.spark.arvo")