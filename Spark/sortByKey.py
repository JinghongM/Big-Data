#SORT BY KEY:
products = sc.textFile("/user/maria_dev/data/retail_db/products")
productsMap = products.filter(lambda p: p.split(",")[4] !=""). \
map(lambda p: (float(p.split(",")[4]),p))
productsSortedByPrice = productsMap.sortByKey()
productsSortedMap = productsSortedByPrice.map(lambda p: p[1].split(",")[1])
----------------------------------------------------
#SORT BY CATEGORY AND BY PRICE
products = sc.textFile("/user/maria_dev/data/retail_db/products")
productsMap = products.filter(lambda p: p.split(",")[4] !=""). \
map(lambda p: ((int(p.split(",")[1]),float(p.split(",")[4])),p))
productsSortedByKey = productsMap.sortByKey("false")
#TRICKY
productsMap = products.filter(lambda p: p.split(",")[4] !=""). \
map(lambda p: ((int(p.split(",")[1]),-float(p.split(",")[4])),p))
productsSortedByKey = productsMap.sortByKey().map(lambda p: p[1])
