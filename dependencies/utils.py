"""
utils.py
~~~~~~~~

Module containing utility functions for use with MOP ETL Job
"""

import dependencies.constants as CONST
import pyspark.sql.functions as F
from pathlib import Path

class Utils:
    def __init__(self, spark):
        self.spark = spark
        self.data_file_folder = CONST.DATA_FOLDER

    def read_csv(self,file):
        """Read CSV files as a Spark DataFrame.
        :param file: Filename of the csv to be read.
        :return: dataframe
        """
        script_file = "/".join([self.data_file_folder, file])
        if not Path(script_file).exists() :
            script_file = "/".join(["..", self.data_file_folder, file])

        return self.spark.read.format("csv").option("header", "true").load(script_file)

    def remove_dummy_columns(selfself, df):
        """Removes the unwanted columns.
        :param df: Dataframe whose unwanted column needs to be removed.
        :return: dataframe
        """
        df_columns = df.columns
        df_columns = [c for c in df_columns if not c.startswith("_c")]
        df = df.select(*df_columns)
        return df

    def get_no_transactions(self, df):
        """Calculates the number of transactions for each household.
        :param df: demograhics Dataframe.
        :return: dataframe
        """
        df = df.withColumn("trans_count", \
                F.when(F.col(CONST.COLUMN_DATE).isNotNull(), 1)\
                .otherwise(0)) \
                .groupBy(CONST.COLUMN_HHID) \
                .agg(F.sum("trans_count").alias(CONST.COLUMN_TRANSACTION_COUNT))
        return df

    def get_trnsaction_amt_before_campaign(self,df):
        """Calculates the total amount of dollars spent before the campaign period for each household.
        :param df: demograhics Dataframe.
        :return: dataframe
        """
        df = df \
            .where(F.col(CONST.COLUMN_DATE) <= F.lit(CONST.CAMPAIGN_START_DATE)) \
            .groupBy(CONST.COLUMN_HHID) \
            .agg(F.round(F.sum("transaction_amount"), 2).alias(CONST.COLUMN_TRANSACTION_AMT_BEFORE_CAMPAIGN))
        return df

    def get_trnsaction_amt_during_campaign(self, df):
        """Calculates the total amount of dollars spent during the campaign period for each household.
        :param df: demograhics Dataframe.
        :return: dataframe
        """
        df = df \
            .where((F.col(CONST.COLUMN_DATE) >= F.lit(CONST.CAMPAIGN_START_DATE)) \
               & (F.col(CONST.COLUMN_DATE) >= F.lit(CONST.CAMPAIGN_END_DATE))) \
            .groupBy(CONST.COLUMN_HHID) \
            .agg(F.round(F.sum("transaction_amount"), 2).alias(CONST.COLUMN_TRANSACTION_AMT_DURING_CAMPAIGN))
        return df

    def get_dataframe_from_spark_sql(self, df,sql_statement):
        """Creates a dataframe based on sql statement passed using spark-sql.
        :param df, sql_statement: Dataframe for temp view and sql statemetn for creating the dataframe
        :return: dataframe
        """
        df.createOrReplaceTempView(CONST.TEMPVIEWNAME)
        sql_statement = sql_statement.replace("df_name", CONST.TEMPVIEWNAME)
        return self.spark.sql(sql_statement)

    def clean_up(self, df):
        """Performs the last set of execution steps like imputing,subset columns and repartitioning.
        :param df: Final dataframe with all the exisiting and derived attributes
        :return: dataframe
        """
        df = df.fillna(0)
        columns_to_drop = [CONST.COLUMN_INDIVIDUAL_ID, CONST.COLUMN_DATE]
        df = df.drop(*columns_to_drop)
        df = df.coalesce(1)

        return df

    def write_to_csv(self, df):
        """Writes the dataframe to a csv file.
        :param df: Final dataframe to be written to output
        :return: None
        """
        df.write.csv(path=CONST.OUTPUT_FOLDER, header="true", mode="overwrite", sep="|")

    def count_missings(self, spark_df, pkey):
        """
        Counts number of nulls and nans in each column
        """
        df = spark_df.select(
            [F.count(F.when(F.isnan(pkey) | F.isnull(pkey), pkey)).alias(pkey)])

        df_count = df.count() - 1
        return df_count