import sys
from functools import reduce
	
def getRevenue(orderItemsList,orderID):
	print(orderID)
	orderItemsFilter = filter(lambda rec: rec.split(",")[1] == orderID, orderItemsList)
	orderItemsMap = map(lambda rec: float(rec.split(",")[4],),orderItemsFilter)
	orderItemsReduce = reduce(lambda total, cur: total + cur, orderItemsMap)
	return orderItemsReduce


orderItemsFile = open("./part-00000")
orderItemsRead = orderItemsFile.read()
orderItemsList = orderItemsRead.splitlines()[:10000]
orderItemsIDMap = set(map(lambda rec: rec.split(",")[1],orderItemsList))
result = dict()
for ID in orderItemsIDMap:
	revenue = getRevenue(orderItemsList,ID)
	result[ID] = revenue
print(result)
