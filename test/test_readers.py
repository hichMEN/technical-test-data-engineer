import unittest
from unittest.mock import patch, MagicMock
import requests
from src.etl.readers import Readers


class TestReaders(unittest.TestCase):

    @patch('requests.get')
    def test_read_df_from_api(self, mock_get):
        # Mock the response from requests.get
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'items': [
                {'id': 1, 'name': 'item1'},
                {'id': 2, 'name': 'item2'}
            ]
        }
        mock_get.return_value = mock_response

        # Mock the Spark session and DataFrame
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df

        # Instantiate the Readers class with the mocked Spark session
        reader = Readers(mock_spark)

        # Call the method
        endpoint = 'http://example.com/api'
        result_df = reader.read_df_from_api(endpoint)

        # Assertions
        mock_get.assert_called_once_with(endpoint, headers={})
        mock_spark.createDataFrame.assert_called_once_with([
            {'id': 1, 'name': 'item1'},
            {'id': 2, 'name': 'item2'}
        ])
        self.assertEqual(result_df, mock_df)

if __name__ == '__main__':
    unittest.main()