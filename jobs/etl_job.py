"""
etl_job.py
~~~~~~~~~~

This Python module contains the actual Moat Outcomes Platform Case Study
ETL job definition. It can be submitted to a Spark cluster
using the 'spark-submit' or can be run locally on Ipython Console
"""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import dependencies
from pyspark.sql.types import TimestampType,IntegerType
from dependencies.spark import start_spark
from dependencies.utils import Utils
import dependencies.constants as CONST

def main():
    try:
        """Main ETL script definition.
    
        :return: None
        """
        # start Spark application and get Spark session, logger and config
        spark, log, config = start_spark(app_name='mop_etl_job', files=['configs/etl_config.json'])

        # log that main ETL job is starting
        log.warn('mop_etl_job is up-and-running')

        # execute ETL pipeline
        u = Utils(spark)

        # read data from csv files into a dataframe
        df_demographics = u.read_csv(CONST.DEMOGRAPHICS_FILE)
        df_hh = u.read_csv(CONST.HOUSEHOLD_FILE)
        df_trans = u.read_csv(CONST.TRANSACTIONS_FILE)
        df_cookie = u.read_csv(CONST.COOKIE_FILE)
        df_activity = u.read_csv(CONST.ACTIVITY_FILE)


        # joining the demographics table with transactions table
        df_demo_hh = df_demographics.join(df_hh, [CONST.COLUMN_HHID], "left").join(df_trans, [CONST.COLUMN_INDIVIDUAL_ID], "left").orderBy(CONST.COLUMN_HHID)

        # Join activity table and cookie table and convert is_reached and date columns to integer and date respectively
        df_activity_cookie = df_activity \
            .join(df_cookie, [CONST.COLUMN_COOKIE_ID]) \
            .withColumn(CONST.COLUMN_IS_REACHED, F.col(CONST.COLUMN_IS_REACHED).cast(IntegerType())) \
            .withColumn(CONST.COLUMN_DATE, F.to_date(F.col(CONST.COLUMN_DATE)))

        print("The number of rows is %d" % (df_activity_cookie.count()))
        ###################### 1. All the demographic features provided  #########################################
        df_demo_hh = u.remove_dummy_columns(df_demo_hh)

        ###################### 2. Number of exposures during the campaign period  ################################
        df_exposures_count = u.get_dataframe_from_spark_sql(df_activity_cookie, CONST.NO_OF_EXPOSURES_SQL_STATEMENT)

        ###################### 3. Number of exposures during the campaign period  ################################
        df_days_since_last_reached = u.get_dataframe_from_spark_sql(df_activity_cookie,
                                                                    CONST.DAYS_SINCE_LAST_REACHED_SQL_STATEMENT)

        ###################### 4. Total amount of dollars spent before the campaign period  ################
        df_demo_hh_amount_before_campaign = u.get_trnsaction_amt_before_campaign(df_demo_hh)

        ###################### 5. Total amount of dollars spent before the campaign period  ################
        df_demo_hh_amount_during_campaign = u.get_trnsaction_amt_during_campaign(df_demo_hh)

        ###################### 6. Total number of transactions  ################################
        df_demo_hh_trans_count = u.get_no_transactions(df_demo_hh)

        ## Joining all the individual tables created above to the main table
        df_indiv_combined = df_exposures_count.join(df_days_since_last_reached, [CONST.COLUMN_INDIVIDUAL_ID], 'inner')

        df_demo_hh_joined = df_demo_hh.join(df_demo_hh_trans_count, [CONST.COLUMN_HHID], 'left') \
            .join(df_demo_hh_amount_before_campaign, [CONST.COLUMN_HHID], 'left') \
            .join(df_demo_hh_amount_during_campaign, [CONST.COLUMN_HHID], 'left')

        df_final = df_demo_hh_joined.join(df_indiv_combined, [CONST.COLUMN_INDIVIDUAL_ID], 'left')

        ## clean up the final table to the format we want
        df_final = u.clean_up(df_final)

        ## Write to CSV
        u.write_to_csv(df_final)

        # log the success and terminate Spark application
        spark.stop()
        
    except Exception as e:
        print("Exception occured")
        print(e)

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
