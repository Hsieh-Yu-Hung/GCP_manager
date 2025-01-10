# GCP_manager

This is a collection of class to handle GCP service functions.

> 特殊符號引發的 SQL bug, 生成 query 字串的時候將特殊符號移除, 目前發現的符號為以下
>
> Illegal symbol in query: `':' '%s' '&'`

## Usage

＊注意要先取得權限並登入！ 使用 `gcloud auth login`, 容器中使用請參考[這篇](https://github.com/ACCUiNBio/BioDB#container%E4%B8%AD%E7%9A%84%E8%BA%AB%E4%BB%BD%E9%A9%97%E8%AD%89)

1. 下載 `GCP_manager` 資料夾, 夾他加入到你的專案中

   ``git clone git@github.com:Hsieh-Yu-Hung/GCP_manager.git``

   ``mv GCP_manager /path/to/your-project/``
2. 導入模組

   ```python
      from GCP_manager.SQL_Manager import SQL_Manager # SQL
      from GCP_manager.GCS_Manager import GCS_Manager # GCS
      from GCP_manager.CRL_Manager import CRL_Manager # Cloud Run
   ```
3. 可以使用模組裡面的功能

## 更新

- 2025-01-19	更新 `CRL_Manager` 上傳檔案到 GCS 之前先檢查, 存在的話先刪除
- 2025-01-09	更新 `CRL_Manager` 判斷參數個數改成判斷以 `$數字` 或 `${數字}` 結尾的行, bash script 超過 9 個參數之後要加大括號！
- 2024-12-17	更新 `SQL_Manager`  當執行 Execute_SQL_Query() 的時候會將 Query 中的特殊符號移除
- 2024-11-26	更新 `CRL_Manager` 可以設定輸出 logs 和 excel 檔案 (excel 是新加資料的詳細, log 是更新資料的詳細)
- 2024-11-26	更新 `CRL_Manager.Update_Database()` 現在會輸出新增的資料為 Excel 並上傳 GCS
- 2024-11-22	更新 `CRL_Manager.process_data()` 可以直接執行 routing 的 Cloud JOB 流程
- 2024-11-12	更新 `SQL-manager.Execute_SQL_Query()` 現在可以加 params 並且回傳查詢結果

## CRL_Manager

＊ 注意使用的 docker image 要確定能夠運行外部輸入的 shell script!

＊ 先測試好, build ＆ push docker image, 才能套用這個 manager!

### Usage

- 搭配以下 `ExecuteProgram.py` 使用, 可以輸入 shell script 和 configure 檔案 (json)
- 通常是在送 JOB 平台中執行這個 manager, 不會單獨使用。 [說明]()
- 執行 Cloud Job routing 流程：

  1. 下載檔案到 Container 中
  2. 執行 shell script 和 additional tasks
  3. 上傳分析結果到 GCS
- `ExecuteProgram.py` 內容如下：

```python
from GCP_manager.CRL_Manager import CRL_Manager
import os
import base64

# 設置環境變量
CONFIGS = os.environ.get("CONFIGS")
SH_SCRIPT = os.environ.get("SH_SCRIPT")

def Decode_Env_Var(env_var):
    if env_var:
        decoded_content = base64.b64decode(env_var).decode('utf-8')
    else:
        print(f"環境變數未設置")
        exit(1)
    return decoded_content

config_content = Decode_Env_Var(CONFIGS)
script_content = Decode_Env_Var(SH_SCRIPT)

if __name__ == "__main__":
    """ 主流程 """
    crl_manager = CRL_Manager(config_content, script_content)
    crl_manager.process_data()
```

## SQL_Manager

This is a small tool for connecting and updating database on Google cloud SQL.

### Usage

＊ 帶有 `params` 的 `Execute_SQL_Query()` 範例, 取得查詢結果:

```python
      # 擷取要搜尋的資訊
  
      # Query
      sql = f"""
        SELECT * FROM {sql_table_name}
        WHERE "Chromosome" = :chrom 
        AND "Position" = :pos 
        AND "REF_MD5" = MD5(:ref)
        AND "ALT_MD5" = MD5(:alt)
      """
  
      # 建立參數字典
      params = {
        'chrom': str(row['CHROM'].replace('chr', '')),
        'pos': str(row['POS']), 
        'ref': str(row['REF']),
        'alt': str(row['ALT'])
      }
  
      res = sql_manager.Execute_SQL_Query(sql_statements=[sql], params=[params])
      column_name = list(res.keys())
  
      # 取得查詢結果
      result = res.fetchall()
```

1. 建立一個 `SQL_Manager`並指定要操作的表格名稱
   ```python
      sql_manager = SQL_Manager(sql_table_name=TABLE_NAME)
   ```
2. 讀取表格成為 `pandas.dataframe()`
   ```python
      col_to_exclude = ['COL1', 'COL2', ...] # 讀取時可以排除一些欄位
      existing_data, preserved_data = sql_manager.Fetch_SQL_Data(db_exclude_columns=col_to_exclude)
   ```
3. 以 dataframe 更新 SQL table
   ```python
      COLUMN_OF_ID = 'column name of the unique ids' # 要指定合併的依據欄位(通常是unique id)
      sql_manager.Update_Database(new_data=NEW_DATA, existing_data=existing_data, column_of_id=COLUMN_OF_ID)
   ```
4. 對當前表格執行現成的 SQL 腳本
   ```python
      # 先指定要替換到腳本裡面的參數使用 SubstituteObject
      from utility.SQL_Manager import SubstituteObject
      sobj = [
         SubstituteObject("__DRUG_DB__", DRUG_DB_NAME),
         SubstituteObject("__TIRGGER__", TRIGGER_NAME),
      ]
      sql_manager.Execute_SQL_Script(SQL_SCRIPT, SQL_ADD_COLS), sobj, sep=';') # 可以指定分行符號
   ```
5. 其他操作
   ```python
      #1. 切換到另一個 SQL table： 
          sql_manager.sql_table_name = "表格名稱"
      #2. 切換至另一個 SQL 連線： 
          sql_manager.SQL_Connect(
              connection_name="連線名稱",
              user_name="使用者名稱",
              user_password="使用者密碼",
              user_db="使用者資料庫"
          )
      #3. 取得 SQL 資料： 
          existing_data, preserved_data = sql_manager.Fetch_SQL_Data(
              db_exclude_columns=排除欄位名稱列表, 
              preserved_data=保留資料物件列表
          )
      #4. 切換觸發器： 
          sql_manager.switch_trigger(
              trigger_name="trigger_name", 
              switch_status=True
          )
      #5. 表格插入新資料： 
          sql_manager.Insert_New_Data(
              added_ids=要加入的id列表, 
              source_dataset=來源資料集, 
              column_of_id=id欄位名稱
          )
      #6. 執行 SQL 語句： 
          sql_manager.Execute_SQL_Query(
              sql_statements=SQL語句列表,
              effected_trigger=被影響到的觸發器名稱列表,
              params=要帶入sql語句的參數,以字典列表儲存
          )
      #7. 比較 SQL 與新的資料差異並回報有差異的 ID： 
          added_ids, updated_ids, deleted_ids = sql_manager.Compare_Difference(
              new_data=新的資料,
              existing_data=現有資料,
              preserved_data=保留的資料
          )
      #8. 生成更新 SQL 語句： 
          update_sql_statements = sql_manager.Generate_Update_SQL_Statements(
              updated_ids=需要更新的ID列表,
              existing_data=現有資料,
              new_data=新的資料
          )
      #9. 生成刪除 SQL 語句： 
          delete_sql_statements = sql_manager.Generate_Delete_SQL_Statements(
              deleted_ids=需要刪除的ID列表
          )
   ```

## GCS_Manager

This is a small tool for downloading files or folders from Google cloud storage.

### Usage

＊注意**遠端**檔案或資料夾路徑 __*不包含*__ Bucket_name !

＊本地檔案不用寫絕對路徑, 可以指定的到就好

＊請參考 `examples/GCS_example.py`裡面的用法

1. 建立一個 `GCS_Manager` 並指定 Bucket, 可以更換

   ```python
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

```python
    manager.download_file(remote_file=遠端檔案, local_file=本地檔案, mode="command_line")
    manager.download_folder(remote_folder=遠端資料夾, local_folder=本地資料夾, mode="command_line")
    manager.upload_file(local_file=本地檔案, remote_file=遠端檔案, mode="command_line")
    manager.upload_folder(remote_folder=本地資料夾, local_folder=遠端資料夾, mode="command_line")
```
