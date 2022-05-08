"""
test_etl_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest

import json

from dependencies.spark import start_spark

from dependencies.utils import Utils
import dependencies.constants as CONST

class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.config = json.loads("""{
                                    "spark.driver.bindAddress": "127.0.0.1",
                                    "spark.sql.autoBroadcastJoinThreshold": -1

                        }""")
        self.spark, *_ = start_spark()

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()



    def test(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        u = Utils(self.spark)

        df_demographics = u.read_csv(CONST.DEMOGRAPHICS_FILE)
        df_hh = u.read_csv(CONST.HOUSEHOLD_FILE)
        df_trans = u.read_csv(CONST.TRANSACTIONS_FILE)
        df_cookie = u.read_csv(CONST.COOKIE_FILE)
        df_activity = u.read_csv(CONST.ACTIVITY_FILE)

        # assert
        self.assertEqual(u.count_missings(df_demographics, CONST.COLUMN_HHID),0)
        self.assertEqual(u.count_missings(df_hh, CONST.COLUMN_HHID),0)
        self.assertEqual(u.count_missings(df_trans, CONST.COLUMN_INDIVIDUAL_ID),0)
        self.assertEqual(u.count_missings(df_cookie, CONST.COLUMN_COOKIE_ID), 0)
        self.assertEqual(u.count_missings(df_activity, CONST.COLUMN_COOKIE_ID), 0)


if __name__ == '__main__':
    unittest.main()
