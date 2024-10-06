import requests
import json

class Readers:
    """
    A class used to read data from an API

    Attributes
    ----------
    spark : SparkSession
        The Spark session used for data processing.

    Methods
    -------
    read_df_from_api(endpoint, **options)
        Reads data from an API endpoint and converts it into a Spark DataFrame.
    """
    def __init__(self, spark):
        self.spark = spark
 
    def read_df_from_api(self, endpoint, **options):
        """
        Reads data from an API endpoint and converts it into a Spark DataFrame.

        Parameters
        ----------
        endpoint : str
            The API endpoint to read data from.
        **options : dict, optional
            Additional options such as headers for the API request.

        Returns
        -------
        DataFrame
            A Spark DataFrame containing the data from the API.
        """
        
        response = requests.get(endpoint, headers=options.get("headers", {}))
        data = response.json()
        
        df = self.spark.createDataFrame(data['items'])
            
        return df