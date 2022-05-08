"""
constants.py
~~~~~~~~

Module containing all the constant values that are used across the MOP ETL script
"""

#File Names
DEMOGRAPHICS_FILE = "demographics.csv"
HOUSEHOLD_FILE = "hh_ind.csv"
TRANSACTIONS_FILE = "transactions.csv"
COOKIE_FILE = "cookie_ind.csv"
ACTIVITY_FILE = "activity.csv"
DATA_FOLDER = "data"
OUTPUT_FOLDER = "output"

#ColumnNames
COLUMN_HHID = 'hhid'
COLUMN_INDIVIDUAL_ID = 'individual_id'
COLUMN_COOKIE_ID = 'cookie_id'
COLUMN_IS_REACHED = 'is_reached'
COLUMN_DATE = 'date'
COLUMN_TRANSACTION_COUNT = "no_of_transactions"
COLUMN_TRANSACTION_AMT_BEFORE_CAMPAIGN = "transaction_amt_before_campaign"
COLUMN_TRANSACTION_AMT_DURING_CAMPAIGN = "transaction_amt_during_campaign"
COLUMN_STATE = 'state'



#Campaign Dates
CAMPAIGN_START_DATE = "2021-09-06"
CAMPAIGN_END_DATE = "2021-09-06"

#Others
TEMPVIEWNAME = "df_tempview"

#SQL Statements
DAYS_SINCE_LAST_REACHED_SQL_STATEMENT = "SELECT b.individual_id, \
                                         bigint((((bigint(to_timestamp(current_date()))) - (bigint(to_timestamp(b.date))))/(3600 * 24))) as days_since_last_reached \
                                         FROM ( \
                                            SELECT a.individual_id, \
                                            a.date, \
                                            row_number() OVER(PARTITION BY a.individual_id ORDER BY a.date DESC) row_number \
                                            FROM \
                                                (SELECT individual_id, date \
                                                FROM df_name \
                                                WHERE is_reached = 1) a )b \
                                            WHERE b.row_number = 1  \
                                            ORDER by b.individual_id"

NO_OF_EXPOSURES_SQL_STATEMENT = "SELECT \
                               individual_id , \
                               sum(is_reached) as no_of_reaches \
                               FROM df_name \
                               GROUP BY individual_id \
                               ORDER BY individual_id"