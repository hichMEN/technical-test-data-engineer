#imports
import yaml
from pyspark.sql import SparkSession


from readers import Readers
from loaders import Loaders


#configs
#spark session
spark = SparkSession.builder.master("local[*]") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

#load etl params
with open('./etl_config.yaml', 'r') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)


reader = Readers(spark)
loader = Loaders(spark)

print(config["sources"])
for source in config["sources"]:

    #Extract
    source_df = reader.read_df_from_api(config['source_uri'] + source['endpoint'])
    source_df.show()

    #load to lakehouse
    loader.load_df_to_lakehouse(source_df, source['delta_path'], source['merge_condition'], source['is_partitionned'], source['partition_id'])


