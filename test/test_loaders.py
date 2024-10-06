import unittest
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import lit
import shutil
import os
from src.etl.loaders import Loaders


class TestLoaders(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]") \
                        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0") \
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                        .getOrCreate()
        cls.loaders = Loaders(cls.spark)
        cls.target_path = "/tmp/delta_table"

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        if os.path.exists(cls.target_path):
            shutil.rmtree(cls.target_path)

    def test_initial_load(self):
        source_df = self.spark.createDataFrame([
            (1, "Alice", "2023-10-01"),
            (2, "Bob", "2023-10-02")
        ], ["id", "name", "updated_at"])

        self.loaders.load_df_to_lakehouse(source_df, self.target_path, "target.id = source.id AND target.is_current = True")

        delta_table = DeltaTable.forPath(self.spark, self.target_path)
        result_df = delta_table.toDF()
        self.assertEqual(result_df.count(), 2)
        self.assertTrue("is_current" in result_df.columns)

    def test_incremental_load(self):
        initial_df = self.spark.createDataFrame([
            (1, "Alice", "2023-10-01"),
            (2, "Bob", "2023-10-02")
        ], ["id", "name", "updated_at"])

        self.loaders.load_df_to_lakehouse(initial_df, self.target_path, "target.id = source.id AND target.is_current = True")

        incremental_df = self.spark.createDataFrame([
            (1, "Alice", "2023-10-03"),
            (3, "Charlie", "2023-10-04")
        ], ["id", "name", "updated_at"])

        self.loaders.load_df_to_lakehouse(incremental_df, self.target_path, "target.id = source.id AND target.is_current = True")

        delta_table = DeltaTable.forPath(self.spark, self.target_path)
        result_df = delta_table.toDF()
        self.assertEqual(result_df.filter(result_df.is_current == True).count(), 3)
        self.assertEqual(result_df.filter(result_df.is_current == False).count(), 1)

    def test_schema_validation(self):
        source_df = self.spark.createDataFrame([
            (1, "Alice", "2023-10-01")
        ], ["id", "name", "updated_at"])

        self.loaders.load_df_to_lakehouse( source_df, self.target_path, "target.id = source.id AND target.is_current = True")

        delta_table = DeltaTable.forPath(self.spark, self.target_path)
        result_df = delta_table.toDF()
        expected_schema = ["id", "name", "updated_at", "is_current"]
        self.assertEqual(result_df.columns, expected_schema)

if __name__ == '__main__':
    unittest.main()