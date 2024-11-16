import pandas as pd
import sys
import snowflake.connector
# from awsglue.utils import getResolvedOptions

con = snowflake.connector.connect(
    user="mohamedzuhairka",
    password="",
    account="JXB38456", #Provide Locator Id
    warehouse="compute_wh",
    database="ECOMMERCE_DB",
    schema="ECOMMERCE_DEV",
    role='ACCOUNTADMIN',
    session_parameters={
        'TIMEZONE': 'UTC',
    }
)

try:
    sql = """
        select * from lineitem limit 10
    """
    data_agg = pd.read_sql(sql, con)
    print(data_agg.head())
finally:
    con.close()