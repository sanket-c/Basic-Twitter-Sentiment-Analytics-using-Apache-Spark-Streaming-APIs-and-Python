from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt


def main():
		conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
		sc = SparkContext(conf=conf)
		ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
		ssc.checkpoint("checkpoint")

		pwords = load_wordlist(sc, "positive.txt")
		nwords = load_wordlist(sc, "negative.txt")
		counts = stream(ssc, pwords, nwords, 100)
		make_plot(counts)


def make_plot(counts):
		"""
		Plot the counts for the positive and negative words for each timestep.
		Use plt.show() so that the plot will popup.
		"""
		p = []
		n = []
		for x in counts:
			p.append(x[0][1])
			n.append(x[1][1])
		plt.plot(p, '-go', label='Positive')
		plt.plot(n, '-ro', label='Negative')
		legend = plt.legend(loc='upper right', shadow=False, fontsize='medium')
		plt.show()



def load_wordlist(sc, filename):
		""" 
		This function should return a list or set of words from the given filename.
		"""
		data = sc.textFile(filename)
		words = set(data.collect())
		return(words)


def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount)


def stream(ssc, pwords, nwords, duration):
		kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
		tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))

		# Each element of tweets will be the text of a tweet.
		# You need to find the count of all the positive and negative words in these tweets.
		# Keep track of a running total counts and print this at every time step (use the pprint function).
		positive = tweets.flatMap(lambda tweet: tweet.split(' ')).filter(lambda word: word in pwords).count()
		p = positive.map(lambda x: ('positive', x))
		negative = tweets.flatMap(lambda tweet: tweet.split(' ')).filter(lambda word: word in nwords).count()
		n = negative.map(lambda x: ('negative', x))

		# Let the counts variable hold the word counts for all time steps
		# You will need to use the foreachRDD function.
		# For our implementation, counts looked like:
		#   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
		counts = []
		# YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
		posNeg = p.union(n)
		posNeg.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))
		runningCount = posNeg.updateStateByKey(updateFunction)
		runningCount.pprint()

		ssc.start()                         # Start the computation
		ssc.awaitTerminationOrTimeout(duration)
		ssc.stop(stopGraceFully=True)

		return counts


if __name__=="__main__":
    main()
