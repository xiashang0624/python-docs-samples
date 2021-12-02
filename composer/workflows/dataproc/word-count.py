# #!/usr/bin/env python

# import pyspark
# import sys

# if len(sys.argv) != 3:
#   raise Exception("Exactly 2 arguments are required: <inputUri> <outputUri>")

# inputUri=sys.argv[1]
# outputUri=sys.argv[2]

# sc = pyspark.SparkContext()
# lines = sc.textFile(sys.argv[1])
# words = lines.flatMap(lambda line: line.split())
# wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda count1, count2: count1 + count2)
# wordCounts.saveAsTextFile(sys.argv[2])

###### Alternative - bq connector
#!/usr/bin/env python

"""BigQuery I/O PySpark example."""

from pyspark.sql import SparkSession

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-demo') \
  .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
bucket = "leah-playground"
spark.conf.set('temporaryGcsBucket', bucket)

# Load data from BigQuery.
words = spark.read.format('bigquery') \
  .option('table', 'bigquery-public-data:samples.shakespeare') \
  .load()
words.createOrReplaceTempView('words')

# Perform word count.
word_count = spark.sql(
    'SELECT word, SUM(word_count) AS word_count FROM words GROUP BY word')
word_count.show()
word_count.printSchema()

# Saving the data to BigQuery
word_count.write.format('bigquery') \
  .option('table', 'wordcount_dataset.wordcount_output') \
  .save()