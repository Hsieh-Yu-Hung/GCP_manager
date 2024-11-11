# GCP-manager
This is a collection of class to handle GCP service functions.

## Usage

＊注意要先取得權限並登入！ 使用 `gcloud auth login`, 容器中使用請參考[這篇](https://github.com/ACCUiNBio/BioDB#container%E4%B8%AD%E7%9A%84%E8%BA%AB%E4%BB%BD%E9%A9%97%E8%AD%89)

1. 下載 `utility` 資料夾, 夾他加入到你的專案中

2. 導入模組

   ```
      from utility.SQL_Manager import SQL_Manager # SQL
      from utility.GCS_Manager import GCS_Manager # GCS
   ```

3. 可以使用模組裡面的功能

## SQL-manager
This is a small tool for connecting and updating database on Google cloud SQL.

### Usage

1. 建立一個`SQL_Manager`並指定要操作的表格名稱
   ```
      sql_manager = SQL_Manager(sql_table_name=TABLE_NAME)
   ```
2. 讀取表格成為 `pandas.dataframe()`
   ```
      col_to_exclude = ['COL1', 'COL2', ...] # 讀取時可以排除一些欄位
      existing_data, preserved_data = sql_manager.Fetch_SQL_Data(db_exclude_columns=col_to_exclude)
   ```
3. 以 dataframe 更新 SQL table
   ```
      COLUMN_OF_ID = 'column name of the unique ids' # 要指定合併的依據欄位(通常是unique id)
      sql_manager.Update_Database(new_data=NEW_DATA, existing_data=existing_data, column_of_id=COLUMN_OF_ID)
   ```
4. 對當前表格執行現成的 SQL 腳本
   ```
      # 先指定要替換到腳本裡面的參數使用 SubstituteObject
      from utility.SQL_Manager import SubstituteObject
      sobj = [
         SubstituteObject("__DRUG_DB__", DRUG_DB_NAME),
         SubstituteObject("__TIRGGER__", TRIGGER_NAME),
      ]
      sql_manager.Execute_SQL_Script(SQL_SCRIPT, SQL_ADD_COLS), sobj, sep=';') # 可以指定分行符號
   ```
5. 其他操作
   ```
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
   ```

## GCS-manager
This is a small tool for downloading files or folders from Google cloud storage.

### Usage

＊注意**遠端**檔案或資料夾路徑 __*不包含*__ Bucket_name !

＊本地檔案不用寫絕對路徑, 可以指定的到就好

＊請參考`examples/GCS_example.py`裡面的用法

1. 建立一個 `GCS_Manager` 並指定 Bucket, 可以更換

   ```
     # 指定 Bucket
     manager = GCS_Manager(bucket_name="TEST-bucket")
     # 更換 Bucket
     managet.set_bucket(new_bucket_name="Another-bucket")
   ```

2. 使用功能

  - 下載單個檔案

    `manager.download_file(remote_file=遠端檔案, local_file=本地檔案)`

  - 下載整個資料夾

    `manager.download_folder(remote_folder=遠端資料夾, local_folder=本地資料夾)`

  - 上傳單個檔案

    `manager.upload_file(local_file=本地檔案, remote_file=遠端檔案)`

  - 上傳整個資料夾

    `manager.upload_folder(remote_folder=本地資料夾, local_folder=遠端資料夾)`

3. 使用 gsutil 指令. 當某些狀況之下無法使用 python package, 可以使用 `gsutil cp` 指令, 只要指定 `mode = "command_line"` 即可！

  ```
    manager.download_file(remote_file=遠端檔案, local_file=本地檔案, mode="command_line")
    manager.download_folder(remote_folder=遠端資料夾, local_folder=本地資料夾, mode="command_line")
    manager.upload_file(local_file=本地檔案, remote_file=遠端檔案, mode="command_line")
    manager.upload_folder(remote_folder=本地資料夾, local_folder=遠端資料夾, mode="command_line")
  ```
