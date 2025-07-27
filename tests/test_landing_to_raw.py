import sys
from unittest import TestCase
from unittest.mock import patch, MagicMock

# Fake pyspark and delta modules so import of landing_to_raw won't fail
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.functions'] = MagicMock()
sys.modules['pyspark.sql.SparkSession'] = MagicMock()
sys.modules['delta'] = MagicMock()
sys.modules['delta.tables'] = MagicMock()

# Now import the function you want to test
from AWS.EMR_Serverless.landing_to_raw import get_all_csv_paths, s3_client, bucket, landing_prefix

class TestGetAllCsvPaths(TestCase):

    @patch.object(s3_client, "list_objects_v2")
    def test_returns_only_csv_files(self, mock_list_objects):
        mock_list_objects.return_value = {
            "Contents": [
                {"Key": f"{landing_prefix}/users/file1.csv"},
                {"Key": f"{landing_prefix}/users/file2.txt"},
                {"Key": f"{landing_prefix}/users/file3.csv"},
            ]
        }

        expected = [
            f"s3://{bucket}/{landing_prefix}/users/file1.csv",
            f"s3://{bucket}/{landing_prefix}/users/file3.csv"
        ]

        result = get_all_csv_paths("users")
        self.assertEqual(result, expected)

if __name__ == "__main__":
    import unittest
    unittest.main()
