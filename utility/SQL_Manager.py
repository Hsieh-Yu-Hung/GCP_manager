# 身份驗證
USER_NAME = 'user1'
USER_PASSWORD = 'user1'
USER_DB = 'postgres'

# SQL
AUTO_UPGRADE = "自動更新"
SQL_CONNECTION_NAME = 'accuinbio-core:asia-east1:db-1'

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='google.auth._default')

import logging
import os
from datetime import datetime

# 設定日誌 : 如果當天已經有日誌，則刪除
log_file_path = f'Update_DB_Record_{datetime.now().strftime("%Y-%m-%d")}.logs'
if os.path.exists(log_file_path): os.remove(log_file_path)
logging.basicConfig(filename=log_file_path, level=logging.INFO)
# 設定日誌格式
class CustomFormatter(logging.Formatter):
    def format(self, record):
        # 設定日期時間格式為 YYYY-MM-DD HH:MM
        self.datefmt = '%Y-%m-%d %H:%M'
        return super().format(record)

# 設定日誌格式
formatter = CustomFormatter('%(asctime)s %(message)s')
for handler in logging.getLogger().handlers:
    handler.setFormatter(formatter)

import pandas as pd
from sqlalchemy import create_engine, text
from google.cloud.sql.connector import Connector
from dataclasses import dataclass

@dataclass
class PreserveObject:
    column_name: str
    value_to_preserve: list
    to_exclude: bool

@dataclass
class SubstituteObject:
    target_name: str
    substitute_value: str

@dataclass
class Updated_Column:
    column_name: str
    old_value: str
    new_value: str

@dataclass
class Edit_Record:
    column_name: str
    record: str

class SQL_Manager:

    """ 說明：
            SQL_Manager 用於管理 SQL 資料庫的連線、資料讀取、更新、寫入、觸發器開關等操作。

        Usage:
            1. 切換到另一個 SQL table： 
                sql_manager.sql_table_name = "表格名稱"
            2. 切換至另一個 SQL 連線： 
                sql_manager.SQL_Connect(
                    connection_name="連線名稱",
                    user_name="使用者名稱",
                    user_password="使用者密碼",
                    user_db="使用者資料庫"
                )
            3. 取得 SQL 資料： 
                existing_data, preserved_data = sql_manager.Fetch_SQL_Data(
                    db_exclude_columns=排除欄位名稱列表, 
                    preserved_data=保留資料物件列表
                )
            4. 切換觸發器： 
                sql_manager.switch_trigger(
                    trigger_name="trigger_name", 
                    switch_status=True
                )
            5. 表格插入新資料： 
                sql_manager.Insert_New_Data(
                    added_ids=要加入的id列表, 
                    source_dataset=來源資料集, 
                    column_of_id=id欄位名稱
                )
            6. 執行 SQL 語句： 
                sql_manager.Execute_SQL_Query(
                    sql_statements=SQL語句列表,
                    effected_trigger=被影響到的觸發器名稱列表
                )
            7. 比較 SQL 與新的資料差異並回報有差異的 ID： 
                added_ids, updated_ids, deleted_ids = sql_manager.Compare_Difference(
                    new_data=新的資料,
                    existing_data=現有資料,
                    preserved_data=保留的資料
                )
            8. 生成更新 SQL 語句： 
                update_sql_statements = sql_manager.Generate_Update_SQL_Statements(
                    updated_ids=需要更新的ID列表,
                    existing_data=現有資料,
                    new_data=新的資料
                )
            9. 生成刪除 SQL 語句： 
                delete_sql_statements = sql_manager.Generate_Delete_SQL_Statements(
                    deleted_ids=需要刪除的ID列表
                )
            10. 執行 SQL 腳本： 
                sql_manager.Execute_SQL_Script(
                    sql_script=SQL腳本路徑,
                    substitute_objects=替換物件列表
                )
            11. 更新資料庫(以 dataframe 更新)： 
                sql_manager.Update_Database(
                    new_data=新的資料(dataframe),
                    existing_data=現有資料(dataframe),
                    preserved_data=保留的資料(dataframe),
                    effected_trigger=被影響到的觸發器名稱列表,
                    edit_record=修改資訊列表
                )
    """

    def __init__(
            self, sql_table_name:str, sql_connection_name=SQL_CONNECTION_NAME,
            user_name=USER_NAME, user_password=USER_PASSWORD, user_db=USER_DB,
        ) -> None:

        # 初始化參數： SQL 設定
        self.user_name = user_name
        self.user_password = user_password
        self.user_db = user_db
        self.sql_table_name = sql_table_name
        self.sql_connection_name = sql_connection_name

        # 嘗試建立 SQL 連線
        self.sql_engine = None
        self.SQL_Connect(
            connection_name=self.sql_connection_name,
            user_name=self.user_name,
            user_password=self.user_password,
            user_db=self.user_db
        )

    def SQL_Connect(self, connection_name:str, user_name:str, user_password:str, user_db:str):
        """ 建立 SQL 連線 """
        # Handle connection
        connector = Connector()
        def getconn():
            conn = connector.connect(
                connection_name,
                "pg8000",                   
                user=user_name,             
                password=user_password,     
                db=user_db,                 
            )
            return conn
        
        # 建立資料庫引擎
        try:
            print(f" --> 嘗試建立連線到 {connection_name}...")
            engine = create_engine(
                f"postgresql+pg8000://{user_name}:{user_password}@{user_db}",
                creator=getconn
            )
            print(f" --> 成功建立連線!")
            self.sql_engine = engine
        except Exception as e:
            print(f" --> 建立連線失敗! Error: {e}")
            self.sql_engine = None

    def Fetch_SQL_Data(self, db_exclude_columns = [], to_preserve:list[PreserveObject] = []):
        """ 取得 SQL 資料 """
        print(f" --> 嘗試讀取 {self.sql_table_name} 資料表...")

        try:    
            # 讀取現有的資料表
            existing_data = pd.read_sql_table(self.sql_table_name, con=self.sql_engine)
            print(f" --> 成功讀取 {self.sql_table_name} 資料表!")
        except Exception as e:
            print(f" --> 讀取資料表失敗! Error: {e}")
            existing_data = None

        preserved_data_list = []
        for each in to_preserve:
            if each.to_exclude:
                preserved_data_list.append(existing_data[~existing_data[each.column_name].isin(each.value_to_preserve)])
            else:
                preserved_data_list.append(existing_data[existing_data[each.column_name].isin(each.value_to_preserve)])
        
        if len(preserved_data_list) > 0:
            preserved_data = pd.concat(preserved_data_list, ignore_index=True)
        else:
            preserved_data = None

        # 移除 db_exclude_columns 欄位
        for each in db_exclude_columns:
            if each in existing_data.columns:
                existing_data = existing_data.drop(columns=[each])

        return existing_data, preserved_data

    def switch_trigger(self, trigger_name:str, switch_status:bool):
        if switch_status:
            # 開啟觸發器
            sql = f"""
            ALTER TABLE {self.sql_table_name} ENABLE TRIGGER {trigger_name};
            """
        else:
            # 關閉觸發器
            sql = f"""
            ALTER TABLE {self.sql_table_name} DISABLE TRIGGER {trigger_name};
            """
        with self.sql_engine.connect() as connection:
            transaction = connection.begin()
            connection.execute(text(sql))
            transaction.commit()

    def Execute_SQL_Query(self, sql_statements:list[str], effected_trigger:list[str]=[]):

        print(f" --> [執行更新] 需要變動 {len(sql_statements)} 行")

        # 關閉觸發器
        for each in effected_trigger:
            self.switch_trigger(each, False)

        """ 批量執行 SQL 更新語句 """
        with self.sql_engine.connect() as connection:
            transaction = connection.begin()                # 開始交易    
            try:
                for i, sql in enumerate(sql_statements):
                    connection.execute(text(sql))           # 使用 text() 來包裝 SQL 字串
                    # 每 10% 回報一次進度比例
                    if len(sql_statements) > 0 and i % max(1, len(sql_statements) // 10) == 0:
                        print(f" --> [執行進度] 已執行了 {i / len(sql_statements) * 100:.2f}%")
                print(f" --> [執行進度] 已執行了 100%")
                transaction.commit()                        # 提交交易
                
            # 如果發生錯誤，回滾交易
            except Exception as e:
                transaction.rollback()  # 回滾交易
                print(f"Error executing statement: {e}")
        print(f" --> [更新變動] 變動了 {len(sql_statements)} 行")

        # 開啟觸發器
        for each in effected_trigger:
            self.switch_trigger(each, True)

    def Insert_New_Data(self, added_ids:list, source_dataset:pd.DataFrame, column_of_id:str, effected_trigger:list[str]=[]):
        """ 插入新資料 """

        print(f" --> [插入新資料] 需要插入 {len(added_ids)} 行")
        logging.info(f" --> [插入新資料] 需要插入 {len(added_ids)} 行")

        # 選取需要插入的資料
        data_to_insert = source_dataset.loc[source_dataset[column_of_id].isin([str(id) for id in added_ids])]

        # 生成 SQL 語句 
        column_names = [ f"\"{col}\"" for col in data_to_insert.columns.tolist()]
        virtual_values = [ f":{vcol.lower()}" for vcol in data_to_insert.columns.tolist()]

        # 對照表
        column_to_virtual_value = dict(zip(virtual_values,column_names))

        # 生成插入語句
        join_column_names = ",".join(column_names)
        join_virtual_values = ",".join(virtual_values)

        sql = f"""INSERT INTO {self.sql_table_name} ({join_column_names}) VALUES ({join_virtual_values});"""

        # 關閉觸發器
        for each in effected_trigger:
            self.switch_trigger(each, False)

        
        for i, each in enumerate(added_ids):
            # 紀錄插入進度
            logging.info(f" --> [插入進度] 已經插入 ID: {each}")

            # 選取需要插入的資料
            selected_row = data_to_insert.loc[data_to_insert[column_of_id] == str(each)]

            # 生成參數
            params = {}
            for each in virtual_values:
                column_name = column_to_virtual_value[each]
                column_name = column_name.replace("\"", "")
                params[each.replace(":", "")] = str(selected_row[column_name].values[0])

            with self.sql_engine.connect() as connection:
                transaction = connection.begin()
                connection.execute(text(sql), params)
                transaction.commit()

            # 每 10% 回報一次進度比例
            if len(added_ids) > 0 and i % max(1, len(added_ids) // 10) == 0:
                print(f" --> [插入進度] 已插入了 {i / len(added_ids) * 100:.2f}%")
                

        print(f" --> [插入新資料] 插入完成")
        logging.info(f" --> [插入新資料] 插入完成")
        # 開啟觸發器
        for each in effected_trigger:
            self.switch_trigger(each, True)

    def Execute_SQL_Script(self, sql_script:str, substitute_objects:list[SubstituteObject]=[], sep=';'):

        # 讀取 SQL 檔案
        with open(sql_script, 'r') as file:
            sql_script = file.read()
        
        # 替換資料表名稱
        for each in substitute_objects:
            sql_script = sql_script.replace(each.target_name, each.substitute_value)

        # 執行 SQL 檔案中的語句
        with self.sql_engine.connect() as connection:
            transaction = connection.begin()  # 開始交易
            try:
                for statement in sql_script.split(sep):
                    if statement.strip():
                        print(f"Executing: {statement.strip()}")  # 打印每個語句
                        connection.execute(text(statement))
                transaction.commit()  # 提交交易
            except Exception as e:
                transaction.rollback()  # 回滾交易
                print(f"Error executing statement: {e}")

    def Compare_Difference(self, new_data:pd.DataFrame, existing_data:pd.DataFrame, column_of_id:str, preserved_data:pd.DataFrame = None):

        """ 比較現有資料和新的資料 """

        print(f" --> [開始偵測] 現有資料有 {existing_data.shape[0]} 行, 新的資料有 {new_data.shape[0]} 行")
        logging.info(f" --> [開始偵測] 現有資料有 {existing_data.shape[0]} 行, 新的資料有 {new_data.shape[0]} 行")

        # 初始化記錄更新、新增、刪除的列表
        updated_ids = []
        added_ids = []
        deleted_ids = []
        preserved_ids = []
        if preserved_data is not None and column_of_id is not None:
            preserved_ids = preserved_data[column_of_id].tolist()

        # 將 new_data 和 existing_data 按照 _Uploaded_variation 排序
        new_data.sort_values(by=column_of_id, inplace=True)
        existing_data.sort_values(by=column_of_id, inplace=True)

        # 排序欄位
        new_data.sort_index(axis=1, inplace=True)
        existing_data.sort_index(axis=1, inplace=True)

        # 如果 existing_data 為空，直接記錄新增的 ID
        if existing_data.empty:
            added_ids.extend(new_data[column_of_id].tolist())
            print(f" --> [首次更新] 新增了 {len(added_ids)} 行")
            logging.info(f" --> [首次更新] 新增了 {len(added_ids)} 行")
        else:

            # 取得 ID 列表
            new_data_ids = new_data[column_of_id].tolist()
            existing_data_ids = existing_data[column_of_id].tolist()

            # 排除 preserved_ids 的資料
            new_data_ids = [each for each in new_data_ids if each not in preserved_ids]
            existing_data_ids = [each for each in existing_data_ids if each not in preserved_ids]

            # 做差集看哪些 ID 需要新增
            added_ids = list(set(new_data_ids) - set(existing_data_ids))

            # 做差集看哪些 ID 需要刪除
            deleted_ids = list(set(existing_data_ids) - set(new_data_ids))

            # 做交集看哪些 ID 需要更新
            updated_ids = set(new_data_ids) & set(existing_data_ids)
            select_new_df = new_data.loc[new_data[column_of_id].isin(updated_ids)]
            select_existing_df = existing_data.loc[existing_data[column_of_id].isin(updated_ids)]

            # 將 select_new_df 和 select_existing_df 數值全部轉換成 string
            select_new_df = select_new_df.astype(str)
            select_existing_df = select_existing_df.astype(str)

            # 比較差異
            diff = pd.concat([select_existing_df, select_new_df]).drop_duplicates(keep=False)
            diff.sort_values(by=column_of_id, inplace=True)
            updated_ids = list(set(diff[column_of_id].tolist()))

            # Write message
            log_message = ""
            for each in added_ids:
                log_message += f"\n--> [新增資料] uniq_identifier: {each}"
            logging.info(f"{log_message}")
        
        # 回報偵測結果
        print(f" --> [偵測完畢] 新增了 {len(added_ids)} 行, 更新了 {len(updated_ids)} 行, 刪除了 {len(deleted_ids)} 行")
        logging.info(f" --> [偵測完畢] 新增了 {len(added_ids)} 行, 更新了 {len(updated_ids)} 行, 刪除了 {len(deleted_ids)} 行")

        # 回傳結果
        return added_ids, updated_ids, deleted_ids

    def Generate_Update_SQL_Statements(self, updated_ids, existing_data, new_data, column_of_id:str):

        def Generate_Update_SQL(table_name, updated_columns, target_id):
            """ 生成 SQL 更新語句 """
            set_clauses = []
            for update in updated_columns:
                # 使用雙引號包裹欄位名稱
                set_clauses.append(f'"{update.column_name}" = \'{update.new_value}\'')
            set_clause = ", ".join(set_clauses)
            sql = f"UPDATE {table_name} SET {set_clause} WHERE \"{column_of_id}\" = '{target_id}';"
            # 加入修改日期
            sql += f'UPDATE {table_name} SET "Modify_date" = \'{datetime.now().strftime("%Y-%m-%d %H:%M")}\' WHERE "{column_of_id}" = \'{target_id}\';'
            # 加入修改原因
            edit_message = f"已更新:"
            for update in updated_columns:
                edit_message += f"\n{update.column_name} : {update.old_value} -> {update.new_value};"
            sql += f'UPDATE {table_name} SET "Modify_record" = \'{edit_message}\' WHERE "{column_of_id}" = \'{target_id}\';'
            
            # 回報更新資料
            log_message = f" --> [更新資料] {target_id} 更新了, "
            for update in updated_columns:
                log_message += f"{update.column_name} : {update.old_value} -> {update.new_value};"
            logging.info(log_message)   
            return sql

        # 將 existing_data 和 new_data 的 ID column 轉換成 string
        existing_data[column_of_id] = existing_data[column_of_id].astype(str)
        new_data[column_of_id] = new_data[column_of_id].astype(str)

        # 生成更新語句
        sql_statements = []
        for each in updated_ids:
            old_data = existing_data.loc[existing_data[column_of_id] == str(each)]
            t_new_data = new_data.loc[new_data[column_of_id] == str(each)]

            # 找出不同的欄位
            different_columns = []
            for column in old_data.columns:
                old_value = str(old_data[column].values[0]).strip()
                new_value = str(t_new_data[column].values[0]).strip()
                
                # 如果舊值和新值不同，則記錄
                if old_value != new_value:
                    update = Updated_Column(column_name=column, old_value=old_value, new_value=new_value)
                    different_columns.append(update)
            
            # 如果不同的欄位存在，則生成更新語句
            if different_columns:
                sql_statements.append(Generate_Update_SQL(self.sql_table_name, different_columns, each))

        return sql_statements

    def Generate_Delete_SQL_Statements(self, deleted_ids, column_of_id:str):
        # 生成刪除語句
        sql_statements = []
        for id_to_delete in deleted_ids:
            sql = f"DELETE FROM \"{self.sql_table_name}\" WHERE \"{column_of_id}\" = '{id_to_delete}';"
            sql_statements.append(sql)
            logging.info(f" --> [刪除資料] 刪除了 {id_to_delete}")
        return sql_statements

    def Update_Database(
            self, new_data:pd.DataFrame, existing_data:pd.DataFrame, 
            column_of_id:str, preserved_data:pd.DataFrame = None, 
            effected_trigger:list[str]=[], edit_record:list[Edit_Record]=[]
        ):
        """ 從 dataframe 更新資料庫 """
        
        # 比較差異
        added_ids, updated_ids, deleted_ids = self.Compare_Difference(new_data, existing_data, column_of_id, preserved_data)

        # 生成更新語句
        update_sql_statements = self.Generate_Update_SQL_Statements(updated_ids, existing_data, new_data, column_of_id)

        # 生成刪除語句
        delete_sql_statements = self.Generate_Delete_SQL_Statements(deleted_ids, column_of_id)

        # 綜合所有語句並執行
        all_sql_statements = update_sql_statements + delete_sql_statements
        self.Execute_SQL_Query(all_sql_statements, effected_trigger=effected_trigger)

        # 插入新資料, 加入修改資訊
        for each in edit_record:
            new_data[each.column_name] = each.record
        self.Insert_New_Data(added_ids, new_data, column_of_id, effected_trigger=effected_trigger)

    def print_sql_info(self):
        print(f"SQL Table Name: {self.sql_table_name}")
        print(f"SQL Connection Name: {self.sql_connection_name}")
        print(f"User Name: {self.user_name}")
        print(f"User Password: {self.user_password}")
        print(f"User DB: {self.user_db}")

if __name__ == "__main__":

    # 印出 SQL_Manager 的說明文件
    help(SQL_Manager)

    # 測試 SQL_Manager
    sql_manager = SQL_Manager(sql_table_name="accurate_db")

    # 取得 SQL 資料
    existing_data, preserved_data = sql_manager.Fetch_SQL_Data()
    print(existing_data)

    # 印出 SQL 資訊
    sql_manager.print_sql_info()
    