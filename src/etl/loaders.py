from pyspark.sql.functions import *
from delta.tables import *

class Loaders:
    """
    A class used to load data
    Attributes
    ----------
    spark : SparkSession
        The Spark session used for data processing.

    Methods
    -------
    load_df_to_lakehouse(source_df, target_path, merge_condition, is_partionned=False, partition_id=None)
        Loads a DataFrame into the Delta Lakehouse, either incrementally or as an initial load.
    """

    def __init__(self, spark):
        self.spark = spark

    def load_df_to_lakehouse(self, source_df, target_path, merge_condition, is_partionned=False, partition_id=None):
        """
        Loads a DataFrame into the Delta Lakehouse.

        Parameters
        ----------
        source_df : DataFrame
            The source DataFrame to be loaded.
        target_path : str
            The target path in the Delta Lakehouse.
        merge_condition : str
            The condition used to merge the source and target DataFrames.
        is_partionned : bool, optional
            Whether the target table should be partitioned (default is False).
        partition_id : str, optional
            The column name to partition by if is_partionned is True (default is None).
        """
        
        source_df.withColumn("is_current", lit(True))
        
        if DeltaTable.isDeltaTable(self.spark,target_path):
            #incremental load
            target_users_dt = DeltaTable.forPath(self.spark,target_path)
            target_users_dt.toDF().show()

            target_users_dt.toDF().alias('target').merge(
                source_df.alias('source'),
                merge_condition)\
                .whenMatchedUpdate( set=
                {
                    "is_current": "False",
                    "updated_at": "source.updated_at"
                }
            ).whenNotMatchedInsertAll()\
            .write.format("delta").mode("append").save(target_path)\
            .execute()

        elif is_partionned:
            #initial load with partition 
            source_df.write.format("delta").mode("overwrite").partitionBy(partition_id).save(target_path)   
        else:
             #initial load without partition 
            source_df.write.format("delta").mode("overwrite").save(target_path)    