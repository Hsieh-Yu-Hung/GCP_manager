# 身份驗證
USER_NAME = 'user1'
USER_PASSWORD = 'user1'
USER_DB = 'postgres'

# SQL
AUTO_UPGRADE = "自動更新"
SQL_CONNECTION_NAME = 'accuinbio-core:asia-east1:db-1'

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module='google.auth._default')
from datetime import datetime
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
                    effected_trigger=被影響到的觸發器名稱列表,
                    params=參數列表
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
        self._logfileName = ""
        self._new_data_added_excel = ""
        self._illegal_symbols = [':', '&', '%s']

        # 嘗試建立 SQL 連線
        self.sql_engine = None
        self.SQL_Connect(
            connection_name=self.sql_connection_name,
            user_name=self.user_name,
            user_password=self.user_password,
            user_db=self.user_db
        )


    # 設定非法符號
    @property
    def illegal_symbols(self):
        return self._illegal_symbols
    @illegal_symbols.setter
    def illegal_symbols(self, value:list[str]):
        self._illegal_symbols = value

    # logfileName 屬性
    @property
    def logfileName(self):
        return self._logfileName
    @logfileName.setter
    def logfileName(self, value):
        if value == "" or type(value) != str:
            raise ValueError("logfileName must be a non-empty string!")
        self._logfileName = value

    # new_data_added_excel 屬性
    @property
    def new_data_added_excel(self):
        return self._new_data_added_excel
    @new_data_added_excel.setter
    def new_data_added_excel(self, value):
        if type(value) != str:
            raise ValueError("new_data_added_excel must be a string!")
        self._new_data_added_excel = value

    def log_record(self, message:str):
        if self.logfileName != "":
            with open(self.logfileName, 'a') as file:
                file.write(f"{message}\n")

    def remove_illegal_symbols(self, symbols:list[str], value:str, replace_with:str=""):
        for each in symbols:
            value = value.replace(each, replace_with)
        return value

    def params_checker(self, params, params_type, warning_message:str=""):
        if type(params) != params_type:
            raise ValueError(f"Type Error: {params} should be {params_type}! {warning_message}")

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

        # Check params
        self.params_checker(db_exclude_columns, list, "若要指定排除欄位, db_exclude_columns 需要是 list[str]!")
        self.params_checker(to_preserve, list, "若要指定保留資料, to_preserve 需要是 list[PreserveObject]!")

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

    def Execute_SQL_Query(self, sql_statements:list[str], effected_trigger:list[str]=[], params:list[dict]=[]):

        # Check params
        self.params_checker(sql_statements, list, "輸入的 sql_statements 需要是列表形式 list[str]!")
        self.params_checker(effected_trigger, list, "若要指定影響的觸發器, effected_trigger 需要是列表形式 list[str]!")
        self.params_checker(params, list, "若要指定參數, params 需要是列表形式 list[dict]!, 參數會依照 sql_statements 的順序填入")

        if len(params) == 0:
            print(f" --> [執行查詢] 需要執行 {len(sql_statements)} 行")

        # 關閉觸發器
        for each in effected_trigger:
            self.switch_trigger(each, False)

        """ 批量執行 SQL 更新語句 """
        query = None
        with self.sql_engine.connect() as connection:
            transaction = connection.begin()  
            try:
                for i, sql in enumerate(sql_statements):
                    if len(params) > 0:
                        query = connection.execute(text(sql), params[i])
                    else:
                        query = connection.execute(text(sql))
                transaction.commit()
                
            # 如果發生錯誤，回滾交易
            except Exception as e:
                transaction.rollback()  # 回滾交易
                print(f"Error executing statement: {e}")
                raise e

        if len(params) == 0:
            print(f" --> [執行查詢] 已成功執行完成")

        # 開啟觸發器
        for each in effected_trigger:
            self.switch_trigger(each, True)
        
        return query

    def Insert_New_Data(self, added_ids:list, source_dataset:pd.DataFrame, column_of_id:str, effected_trigger:list[str]=[]):
        """ 插入新資料 """

        print(f" --> [插入新資料] 需要插入 {len(added_ids)} 行")
        self.log_record(f" --> [插入新資料] 需要插入 {len(added_ids)} 行")

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
            self.log_record(f" --> [插入進度] 已經插入 ID: {each}")

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
        self.log_record(f" --> [插入新資料] 插入完成")
        # 開啟觸發器
        for each in effected_trigger:
            self.switch_trigger(each, True)

    def Execute_SQL_Script(self, sql_script:str, substitute_objects:list[SubstituteObject]=[], sep=';'):

        # Check params
        self.params_checker(sql_script, str, "輸入的 sql_script 需要是字串形式 str!")
        self.params_checker(substitute_objects, list, "若要指定替換物件, substitute_objects 需要是列表形式 list[SubstituteObject]!")

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
                        connection.execute(text(statement))
                transaction.commit()  # 提交交易
            except Exception as e:
                transaction.rollback()  # 回滾交易
                print(f"Error executing statement: {e}")

    def Compare_Difference(self, new_data:pd.DataFrame, existing_data:pd.DataFrame, column_of_id:str, preserved_data:pd.DataFrame = None):

        """ 比較現有資料和新的資料 """

        print(f" --> [開始偵測] 現有資料有 {existing_data.shape[0]} 行, 新的資料有 {new_data.shape[0]} 行")
        self.log_record(f" --> [開始偵測] 現有資料有 {existing_data.shape[0]} 行, 新的資料有 {new_data.shape[0]} 行")

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
            self.log_record(f" --> [首次更新] 新增了 {len(added_ids)} 行")
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
            self.log_record(f"{log_message}")
        
        # 回報偵測結果
        print(f" --> [偵測完畢] 新增了 {len(added_ids)} 行, 更新了 {len(updated_ids)} 行, 刪除了 {len(deleted_ids)} 行")
        self.log_record(f" --> [偵測完畢] 新增了 {len(added_ids)} 行, 更新了 {len(updated_ids)} 行, 刪除了 {len(deleted_ids)} 行")

        # 回傳結果
        return added_ids, updated_ids, deleted_ids

    def Generate_Update_SQL_Statements(self, updated_ids, existing_data, new_data, column_of_id:str, added_cols:list[str]=[], removed_cols:list[str]=[]):

        def Generate_Update_SQL(table_name, updated_columns, target_id):
            """ 生成 SQL 更新語句 """
            set_clauses = []
            for update in updated_columns:
                # 使用雙引號包裹欄位名稱, 移除 symbols
                update.new_value = self.remove_illegal_symbols(self.illegal_symbols, update.new_value, replace_with="-")
                set_clauses.append(f'"{update.column_name}" = \'{update.new_value}\'')
            set_clause = ", ".join(set_clauses)
            sql = f"UPDATE {table_name} SET {set_clause} WHERE \"{column_of_id}\" = '{target_id}';"
            # 加入修改日期
            sql += f'UPDATE {table_name} SET "Modify_date" = \'{datetime.now().strftime("%Y-%m-%d %H:%M")}\' WHERE "{column_of_id}" = \'{target_id}\';'
            # 加入修改原因
            edit_message = f"已更新:"
            for update in updated_columns:
                update.old_value = self.remove_illegal_symbols(self.illegal_symbols, update.old_value, replace_with="-")
                edit_message += f"\n{update.column_name} 從 {update.old_value} 更新到 {update.new_value};"
            sql += f'UPDATE {table_name} SET "Modify_record" = \'{edit_message}\' WHERE "{column_of_id}" = \'{target_id}\';'
            
            # 回報更新資料
            log_message = f" --> [更新資料] {target_id} 更新了, "
            for update in updated_columns:
                log_message += f"{update.column_name} 從 {update.old_value} 更新到 {update.new_value};"
            self.log_record(log_message)   

            return sql

        # 將 existing_data 和 new_data 的 ID column 轉換成 string
        existing_data[column_of_id] = existing_data[column_of_id].astype(str)
        new_data[column_of_id] = new_data[column_of_id].astype(str)

        # 生成更新語句
        sql_statements = []
        for each in updated_ids:
            old_data = existing_data.loc[existing_data[column_of_id] == str(each)]
            t_new_data = new_data.loc[new_data[column_of_id] == str(each)]

            # 決定搜索欄位範圍 如果 added_cols 和 removed_cols 則增減搜索欄位
            search_columns = old_data.columns.tolist()
            if len(added_cols) > 0:
                search_columns.extend(added_cols)
            if len(removed_cols) > 0:
                search_columns = [each for each in search_columns if each not in removed_cols]

            # 找出不同的欄位
            different_columns = []
            for column in search_columns:
                old_value = str(old_data[column].values[0]).strip() if column in old_data.columns.tolist() else ""
                new_value = str(t_new_data[column].values[0]).strip() if column in t_new_data.columns.tolist() else ""
                
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
            self.log_record(f" --> [刪除資料] 刪除了 {id_to_delete}")
        return sql_statements

    def Update_Database(
            self, new_data:pd.DataFrame, existing_data:pd.DataFrame, 
            column_of_id:str, preserved_data:pd.DataFrame = None, 
            effected_trigger:list[str]=[], edit_record:list[Edit_Record]=[]
        ):
        """ 從 dataframe 更新資料庫 """

        # 比較新舊 Data 的欄位, 如果有新增則加入, 如果有減少則刪除
        added_columns = list(set(new_data.columns.tolist()) - set(existing_data.columns.tolist()))
        removed_columns = list(set(existing_data.columns.tolist()) - set(new_data.columns.tolist()))
        add_column_sql = [f"ALTER TABLE {self.sql_table_name} ADD COLUMN \"{each}\" TEXT;" for each in added_columns]
        remove_column_sql = [f"ALTER TABLE {self.sql_table_name} DROP COLUMN \"{each}\";" for each in removed_columns]
        self.Execute_SQL_Query(add_column_sql + remove_column_sql)
        
        # 比較差異
        added_ids, updated_ids, deleted_ids = self.Compare_Difference(new_data, existing_data, column_of_id, preserved_data)

        # 生成更新語句
        update_sql_statements = self.Generate_Update_SQL_Statements(updated_ids, existing_data, new_data, column_of_id, added_cols=added_columns, removed_cols=removed_columns)

        # 生成刪除語句
        delete_sql_statements = self.Generate_Delete_SQL_Statements(deleted_ids, column_of_id)

        # 綜合所有語句並執行
        all_sql_statements = update_sql_statements + delete_sql_statements
        self.Execute_SQL_Query(all_sql_statements, effected_trigger=effected_trigger)
        
        # 插入新資料, 加入修改資訊
        for each in edit_record:
            new_data[each.column_name] = each.record
        self.Insert_New_Data(added_ids, new_data, column_of_id, effected_trigger=effected_trigger)

        # 從 newData 中搜集新增的資料
        if self.new_data_added_excel != "":
            new_data_added = new_data.loc[new_data[column_of_id].isin(added_ids)]
            new_data_added.to_excel(self.new_data_added_excel, index=False)
        
    def print_sql_info(self):
        print(f"SQL Table Name: {self.sql_table_name}")
        print(f"SQL Connection Name: {self.sql_connection_name}")
        print(f"User Name: {self.user_name}")
        print(f"User Password: {self.user_password}")
        print(f"User DB: {self.user_db}")
        print(f"New Data Added Excel: {self.new_data_added_excel}")
        if self.logfileName != "":
            print(f"Log File: {self.logfileName}")
        else:
            print("Log File: None")

if __name__ == "__main__":

    # 印出 SQL_Manager 的說明文件
    # help(SQL_Manager)

    # 測試 SQL_Manager
    sql_manager = SQL_Manager(sql_table_name="accurate_db")

    # 取得 SQL 資料
    existing_data, preserved_data = sql_manager.Fetch_SQL_Data()
    print(existing_data)

    # 印出 SQL 資訊
    sql_manager.print_sql_info()
    