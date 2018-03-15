#MAP
#use map to build a RDD of tuples
orderItems = sc.textFile("file:///home/maria_dev/cca-175/retail_db/order_items/part-00000")
orderItemsMap = orderItems.map(lambda orderItem: (int(orderItem.split(",")[1]),float(orderItem.split(",")[4])))
for i in orderItemsMap.take(10):
	print i
//
(1, 299.98000000000002)
(2, 199.99000000000001)
(2, 250.0)
(2, 129.99000000000001)
(4, 49.979999999999997)
(4, 299.94999999999999)
(4, 150.0)
(4, 199.91999999999999)
(5, 299.98000000000002)
(5, 299.94999999999999)
#use flat map to build a RDD
lineList = ["How are you","let us perform","word count using flatMap","to understand flatMap in detail"]
lines = sc.parallelize(lineList)
words = lines.flatMap(lambda l: l.split(" "))
for i in words.collect(): print(i)
//How
are
you
let
us
perform
word
count
using
flatMap
to
understand
flatMap
in
detail
words = lines.map(lambda l: l.split(" "))
for i in words.collect(): print(i)
['How', 'are', 'you']
['let', 'us', 'perform']
['word', 'count', 'using', 'flatMap']
['to', 'understand', 'flatMap', 'in', 'detail']