import dlt

from pyspark.sql import SparkSession

data_folder = './data/users.csv'
# Initialize a SparkSession

spark = SparkSession.builder \
    .appName("ReadCSVExample")\
    .master("local[*]") \
    .getOrCreate()


sparkdf = spark.read.format("csv") \
    .option("header", "true") \
    .load(data_folder)
sparkdf.printSchema()

pandasdf = sparkdf.toPandas()
data  = pandasdf.to_dict(orient='records')

pipeline = dlt.pipeline(
    pipeline_name="dlt_pipeline2", destination="duckdb", dataset_name="users"
)
load_info = pipeline.run(data, table_name="users")