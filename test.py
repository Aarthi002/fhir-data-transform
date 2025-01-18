# test_fhir_data_transform.py

import unittest
from unittest.mock import patch, MagicMock
from recon import write_to_postgres

class TestFhirDataTransform(unittest.TestCase):

    @patch('recon.SparkSession.builder.getOrCreate')  # Mock SparkSession
    def test_write_to_postgres_success(self, mock_spark_session):
        # Mock DataFrame and properties
        mock_spark = mock_spark_session.return_value
        mock_df = MagicMock()
        resource_type = "Patient"
        
        # Mock write operation to be successful
        mock_df.write.jdbc.return_value = None
        
        # Call the function and assert success
        result = write_to_postgres(mock_df, resource_type)
        self.assertTrue(result)
    
    @patch('recon.SparkSession.builder.getOrCreate')  # Mock SparkSession
    def test_write_to_postgres_failure(self, mock_spark_session):
        # Mock DataFrame and properties
        mock_spark = mock_spark_session.return_value
        mock_df = MagicMock()
        resource_type = "Patient"
        
        # Mock write operation to fail
        mock_df.write.jdbc.side_effect = Exception("Database write error")
        
        # Call the function and assert failure
        result = write_to_postgres(mock_df, resource_type)
        self.assertFalse(result)

if __name__ == '__main__':
    unittest.main()
