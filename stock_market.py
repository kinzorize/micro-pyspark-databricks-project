from pyspark.sql.functions import month
from pyspark.sql.functions import year
from pyspark.sql.functions import corr
from pyspark.sql.functions import count
from pyspark.sql.functions import mean, min, max
from pyspark.sql.functions import format_number
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Final').getOrCreate()
df = spark.read.format("csv").options(header="true", inferSchema="true").load(
    "dbfs:/FileStore/shared_uploads/barrackoboma2015@gmail.com/walmart_stock.csv")

df.show()

df.printSchema()

df.head(5)

for row in df.head(5):
    print(row)
    print('\n')

df.describe().show()

df.describe().printSchema()

result = df.describe()

result.select(result['summary'], format_number(result['Open'].cast('float'), 2).alias('Open'),
              format_number(result['High'].cast('float'), 2).alias('High'),
              format_number(result['Low'].cast('float'), 2).alias('Low'),
              format_number(result['Close'].cast('float'), 2).alias('Close'),
              result['Volume'].cast('int').alias('Volume')).show()
df2 = df.withColumn('HV Ratio', df['High']/df['Volume'])
df2.select('HV Ratio').show()

df.orderBy(df['High'].desc()).head(1)

df.orderBy(df['High'].desc()).show()

df.select(mean('Close').alias('Average of Close')).show()

df.select(max('Volume').alias("Maximum Volume"),
          min('Volume').alias("Minimum Volume")).show()

df.filter('Close < 60').count()

df.filter(df['Close'] < 60).count()

result = df.filter(df['Close'] < 60)
result.select(count('Close').alias('Total Close')).show()

(df.filter(df['High'] > 80).count() / df.count()) * 100

df.select(corr('High', 'Volume')).show()


df.select(corr('High', 'Volume').alias('correlation of High & Volume')).show()

yeardf = df.withColumn("Year", year(df['Date']))

max_df = yeardf.groupBy('Year').max()
max_df.select('Year', 'max(High)').show()

monthdf = df.withColumn('Month', month('Date'))

monthavgs = monthdf.select(['Month', 'Close']).groupBy('Month').mean()

monthavgs.select('Month', 'avg(Close)').orderBy('Month').show()

df.show()
