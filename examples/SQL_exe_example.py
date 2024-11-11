# 參數
DRUG_DB_NAME = 'drug_db'
BIO_DB_NAME = 'accurate_db'
BIO_DB_ID = 'uniq_identifier'

# 執行 SQL 腳本位置
import os
SQL_SCRIPT_PATH = os.path.join(os.path.dirname(__file__), 'query')
SQL_BUILD_TABLE = 'drug_db_schema.sql'

from utility.SQL_Manager import SQL_Manager, SubstituteObject

# 建立 SQL_Manager 物件
sql_manager = SQL_Manager(sql_table_name=DRUG_DB_NAME)

# 執行 SQL 腳本
to_replace = [
    SubstituteObject('__DRUG_DB_NAME__', DRUG_DB_NAME),
    SubstituteObject('__BIO_DB_NAME__', BIO_DB_NAME),
    SubstituteObject('__BIO_DB_ID__', BIO_DB_ID)
]
sql_manager.Execute_SQL_Script(os.path.join(SQL_SCRIPT_PATH, SQL_BUILD_TABLE), to_replace)

# 印出資料
drug_db_data, _ = sql_manager.Fetch_SQL_Data()
print(drug_db_data.columns)