import os
from dotenv import load_dotenv
from snowflake.snowpark import Session

load_dotenv()

ACCOUNT = os.getenv('ACCOUNT')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
ROLE = os.getenv('ROLE')
WAREHOUSE = os.getenv('WAREHOUSE')
DATABASE = os.getenv('DATABASE')
SCHEMA = os.getenv('SCHEMA')

connection_parameters = {
    'account': ACCOUNT,
    'user': USER,
    'password': PASSWORD,
    'role': ROLE,
    'warehouse': WAREHOUSE,
    'database': DATABASE,
    'schema': SCHEMA
}

test_session = Session.builder.configs(connection_parameters).create()

test_query = """
SELECT * 
FROM event
WHERE day = DATE('2022-06-01')
LIMIT 10
"""
query_results = test_session.sql(test_query).collect()
print(query_results) 
test_session.close()