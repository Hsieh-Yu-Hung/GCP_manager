# 參數
DRUG_DB_NAME = 'drug_db'
COLUMN_OF_ID = 'drug_db_id'

# GCS 參數
GCS_BUCKET_NAME = 'accuinbio-core-dev'
GCS_FILE_PATH = 'TEST_VCF_ANNO/藥物邏輯v2_20241106.xlsx'
LOCAL_SAVE_PATH = './藥物邏輯v2_20241106.xlsx'

# 執行 SQL 腳本位置
import os
SQL_SCRIPT_PATH = os.path.join(os.path.dirname(__file__), 'query')
SQL_ADD_COLS = 'drug_db_addCols.sql'
SQL_UPDATE_RELATION = 'relation_db_update.sql'

from utility.SQL_Manager import SQL_Manager, SubstituteObject
from utility.GCS_Manager import GCS_Manager
import pandas as pd
if __name__ == "__main__":

    # 下載 GCS 資料
    gcs_manager = GCS_Manager(bucket_name=GCS_BUCKET_NAME)
    gcs_manager.download_file(remote_file=GCS_FILE_PATH, local_file=LOCAL_SAVE_PATH)

    # 讀取 Excel 資料
    new_drug_data = pd.read_excel(LOCAL_SAVE_PATH)

    # 建立 SQL_Manager 物件
    sql_manager = SQL_Manager(sql_table_name=DRUG_DB_NAME)

    # 取得 SQL 資料
    db_exclude_columns = ['Modify_record', 'Modify_date','POS_Type','POS_numeric','POS_Change']
    existing_data,_ = sql_manager.Fetch_SQL_Data(db_exclude_columns=db_exclude_columns)

    # 更新資料庫
    sql_manager.Update_Database(new_data=new_drug_data, existing_data=existing_data, column_of_id=COLUMN_OF_ID)

    # 執行 GenerateMutInfo.sql
    sobj = [SubstituteObject("__DRUG_DB__", DRUG_DB_NAME)]
    sql_manager.Execute_SQL_Script(os.path.join(SQL_SCRIPT_PATH, SQL_ADD_COLS), sobj)

    # 執行 Update_relation_db.sql
    sobj.extend([
        SubstituteObject("__BIO_DB__", 'accurate_db'),
        SubstituteObject("__DB_RELATION__", "db_relation")
    ])
    sql_manager.Execute_SQL_Script(os.path.join(SQL_SCRIPT_PATH, SQL_UPDATE_RELATION), sobj)

    print(f" --> [更新完成] 更新完成")  