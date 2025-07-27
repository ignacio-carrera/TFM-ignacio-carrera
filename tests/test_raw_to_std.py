import sys
import unittest
from unittest.mock import patch, MagicMock

sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.functions'] = MagicMock()
sys.modules['pyspark.sql.window'] = MagicMock()
sys.modules['delta'] = MagicMock()
sys.modules['delta.tables'] = MagicMock()

from AWS.EMR_Serverless import raw_to_std

class TestRawToStdFunctions(unittest.TestCase):

    def setUp(self):
        self.mock_df = MagicMock()
        self.mock_df.filter.return_value = self.mock_df
        self.mock_df.withColumn.return_value = self.mock_df

        # transform calls its lambda argument with mock_df and returns result
        def transform_side_effect(func):
            return func(self.mock_df)
        self.mock_df.transform.side_effect = transform_side_effect

        # Mock col() output
        self.mock_col_obj = MagicMock()
        self.mock_col_obj.isNotNull.return_value = True
        self.mock_col_obj.__ge__.return_value = True
        raw_to_std.col = MagicMock(return_value=self.mock_col_obj)

    def test_clean_users(self):
        with patch.dict(raw_to_std.clean_users.__globals__, {'deduplicate_latest': MagicMock(return_value='deduped_df')}):
            result = raw_to_std.clean_users(self.mock_df)
            self.assertEqual(result, 'deduped_df')
            raw_to_std.clean_users.__globals__['deduplicate_latest'].assert_called_once_with(self.mock_df, ['id'])

    def test_clean_billable_hours(self):
        with patch.dict(raw_to_std.clean_billable_hours.__globals__, {'deduplicate_latest': MagicMock(return_value='deduped_df')}):
            result = raw_to_std.clean_billable_hours(self.mock_df)
            self.assertEqual(result, 'deduped_df')
            raw_to_std.clean_billable_hours.__globals__['deduplicate_latest'].assert_called_once_with(self.mock_df, ['userId', 'start_date', 'end_date'])

if __name__ == "__main__":
    unittest.main()
