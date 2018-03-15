#Get top N products by price - Global Ranking -top or takeOrdered
products = sc.textFile("/user/maria_dev/data/retail_db/products")
productsFiltered = products.filter(lambda p: p.split(",")[4] != "")
topNProducts = productsFiltered.top(5,key = lambda k:(int(k.split(",")[1]), float(k.split(",")[4])))
topNProducts = productsFiltered.takeOrdered(5,key = lambda k: -float(k.split(",")[4]))