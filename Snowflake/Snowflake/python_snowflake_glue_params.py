import sys
import snowflake.connector
from awsglue.utils import getResolvedOptions
from datetime import datetime,timedelta

args = getResolvedOptions(sys.argv, ['supplier_key','ship_date'])

supplier_key = args['supplier_key']
ship_date = args['ship_date']

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

cursor = con.cursor()

sql_query = """
            select 
            * 
            from lineitem 
            where l_shipdate='{0}' and l_suppkey='{1}'
            """.format(ship_date,supplier_key)

try : 
    cursor.execute(sql_query)
    query_id = cursor.sfqid
    cursor.get_results_from_sfqid(query_id)
    results = cursor.fetchall()
    print(f'{results[0]}')
    
except Exception as e:
    con.rollback()
    raise e
    
finally:
    con.close() 