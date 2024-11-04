# GCS-manager
This is a small tool for downloading files or folders from Google cloud storage.

## Usage

＊注意要先取得權限並登入！ 使用 `gcloud auth login`, 容器中使用請參考[這篇](https://github.com/ACCUiNBio/BioDB#container%E4%B8%AD%E7%9A%84%E8%BA%AB%E4%BB%BD%E9%A9%97%E8%AD%89)

＊注意**遠端**檔案或資料夾路徑 __*不包含*__ Bucket_name !

＊本地檔案不用寫絕對路徑, 可以指定的到就好

＊請參考`example.py`裡面的用法

1. 將 GCS_Manager.py 複製到專案資料夾中
2. 導入 `GCS_Manager` 並指定 Bucket, 可以更換

   ```
     from GCS_Manager import GCS_Manager
     # 指定 Bucket
     manager = GCS_Manager(bucket_name="TEST-bucket")
     # 更換 Bucket
     managet.set_bucket(new_bucket_name="Another-bucket")
   ```

3. 使用功能

  - 下載單個檔案

    `manager.download_file(remote_file=遠端檔案, local_file=本地檔案)`

  - 下載整個資料夾

    `manager.download_folder(remote_folder=遠端資料夾, local_folder=本地資料夾)`

  - 上傳單個檔案

    `manager.upload_file(local_file=本地檔案, remote_file=遠端檔案)`

  - 上傳整個資料夾

    `manager.upload_folder(remote_folder=本地資料夾, local_folder=遠端資料夾)`

4. 使用 gsutil 指令. 當某些狀況之下無法使用 python package, 可以使用 `gsutil cp` 指令, 只要指定 `mode = "command_line"` 即可！

  ```
    manager.download_file(remote_file=遠端檔案, local_file=本地檔案, mode="command_line")
    manager.download_folder(remote_folder=遠端資料夾, local_folder=本地資料夾, mode="command_line")
    manager.upload_file(local_file=本地檔案, remote_file=遠端檔案, mode="command_line")
    manager.upload_folder(remote_folder=本地資料夾, local_folder=遠端資料夾, mode="command_line")
  ```
