#Get top N priced products - By key Ranking
#Using groupByKey and flatMap
products = sc.textFile("/user/maria_dev/data/retail_db/products")
proproductsFiltered = products.filter(lambda p: p.split(",")[4] != "")
productsMap = productsFiltered.map(lambda p: (int(p.split(",")[1]),p))
productsGroupByCategoryId = productsMap.groupByKey()
t = productsGroupByCategoryId.filter(lambda p: p[0] == 59).first() #products itervative of categoryId = 59
def getTopNPricedProductsPerCategoryId(productCategoryId,topN):
	productSorted = sorted(productCategoryId[1],key = lambda p: float(p.split(",")[4]),reverse = True) #All products sorted by Price
	productPrices = map(lambda p: float(p.split(",")[4]), productSorted) #all prices
	topNPrices =sorted(set(productPrices),reverse = True) [:topN] #top 3 prices
	import itertools as it
	return it.takewhile(lambda p: float(p.split(",")[4]) in topNPrices,productSorted) #all products in top 3 prices

topNPricedProducts = productsGroupByCategoryId.flatMap(lambda p: getTopNPricedProductsPerCategoryId(p,3))
