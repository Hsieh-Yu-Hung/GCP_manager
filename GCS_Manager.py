import warnings
import os
from google.cloud import storage

# 抑制特定的 UserWarning
warnings.filterwarnings("ignore", category=UserWarning, module='google.auth._default')

# Transfer MODE
_COMMAND_LINE = 'command_line'
_PYTHON = 'python'

class GCS_Manager:
    
    """
        Class to manage the GCS bucket

        Usage:
            1. 建構 manager = GCS_Manager(bucket_name=Bucket 名稱)
            2. 印出 GCS 資訊 manager.print_GCS_info()
            3. 切換 Bucket manager.set_bucket(new_bucket_name=另一個 Bucket 名稱)
            4. 下載檔案 manager.download_file(remote_file=遠端路徑, local_file=本地路徑, mode='python'或'command_line')
            5. 上傳檔案 manager.upload_file(local_file=本地路徑, remote_file=遠端路徑, mode='python'或'command_line')
            6. 下載資料夾 manager.download_folder(remote_folder=遠端路徑, local_folder=本地路徑, mode='python'或'command_line')
            7. 上傳資料夾 manager.upload_folder(local_folder=本地路徑, remote_folder=遠端路徑, mode='python'或'command_line')
    """

    def __init__(self, bucket_name):

        """ 建構式"""

        # Bucket name
        self.bucket_name = bucket_name

        # Client
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)


    def download_file(self, remote_file, local_file, mode=_PYTHON):
        """ 下載單個檔案 """
        if mode == _PYTHON:
            blob = self.bucket.blob(remote_file)
            blob.download_to_filename(local_file)
            print(f" --> [下載檔案] \n --> 遠端檔案：{remote_file} \n --> 下載到：{local_file} \n")
        elif mode == _COMMAND_LINE:
            os.system(f"gsutil -m cp gs://{self.bucket_name}/{remote_file} {local_file}")
            print(f" --> [下載檔案] \n --> 遠端檔案：{remote_file} \n --> 下載到：{local_file} \n")

    def download_folder(self, remote_folder, local_folder, mode=_PYTHON):
        """ 下載整個資料夾 """
        if mode == _PYTHON:
            # 確保本地目錄存在
            if not os.path.exists(local_folder):
                os.makedirs(local_folder)
            
            for blob in self.bucket.list_blobs(prefix=remote_folder):
                # 確保子目錄存在
                local_file_path = os.path.join(local_folder, blob.name)
                local_file_dir = os.path.dirname(local_file_path)
                if not os.path.exists(local_file_dir):
                    os.makedirs(local_file_dir)
                
                blob.download_to_filename(local_file_path)
                print(f" --> [下載檔案] \n --> 遠端檔案：{blob.name} \n --> 下載到：{local_folder}/{blob.name} \n")
        elif mode == _COMMAND_LINE:
            os.system(f"gsutil -m cp -r gs://{self.bucket_name}/{remote_folder} {local_folder}")
            print(f" --> [下載資料夾] \n --> 遠端資料夾：{remote_folder} \n --> 下載到：{local_folder} \n")

    def upload_file(self, local_file, remote_file, mode=_PYTHON):
        """ 上傳單個檔案 """
        if mode == _PYTHON:
            blob = self.bucket.blob(remote_file)
            blob.upload_from_filename(local_file)
            print(f" --> [上傳檔案] \n --> 本地檔案：{local_file} \n --> 上傳到：{remote_file} \n")
        elif mode == _COMMAND_LINE:
            os.system(f"gsutil -m cp {local_file} gs://{self.bucket_name}/{remote_file}")
            print(f" --> [上傳檔案] \n --> 本地檔案：{local_file} \n --> 上傳到：{remote_file} \n")

    def upload_folder(self, local_folder, remote_folder, mode=_PYTHON):
        """ 上傳整個資料夾 """
        if mode == _PYTHON:
            for root, _, files in os.walk(local_folder):
                for file_name in files:
                    local_file_path = os.path.join(root, file_name)
                    relative_path = os.path.relpath(local_file_path, local_folder)
                    destination_blob_name = os.path.join(remote_folder, relative_path)

                    # 上傳檔案
                    blob = self.bucket.blob(destination_blob_name)
                    blob.upload_from_filename(local_file_path)
                    print(f" --> [上傳檔案] \n --> 本地檔案：{local_file_path} \n --> 上傳到：{destination_blob_name} \n")
        elif mode == _COMMAND_LINE:
            os.system(f"gsutil -m cp -r {local_folder} gs://{self.bucket_name}/{remote_folder}")
            print(f" --> [上傳資料夾] \n --> 本地資料夾：{local_folder} \n --> 上傳到：{remote_folder} \n")

    def check_file_exists(self, remote_path):
        """ 檢查遠端檔案是否存在 """
        blob = self.bucket.blob(remote_path)
        return blob.exists()

    def delete_remote_folder(self, remote_folder, mode=_PYTHON):
        """ 刪除遠端資料夾 """
        if mode == _PYTHON:
            for blob in self.bucket.list_blobs(prefix=remote_folder):
                blob.delete()
                print(f" --> [刪除檔案] \n --> 遠端檔案：{blob.name} \n")
        elif mode == _COMMAND_LINE:
            os.system(f"gsutil -m rm -rf gs://{self.bucket_name}/{remote_folder}")
            print(f" --> [刪除資料夾] \n --> 遠端資料夾：{remote_folder} \n")

    def delete_remote_file(self, remote_file, mode=_PYTHON):
        """ 刪除遠端檔案 """
        if mode == _PYTHON:
            blob = self.bucket.blob(remote_file)
            blob.delete()
            print(f" --> [刪除檔案] \n --> 遠端檔案：{remote_file} \n")
        elif mode == _COMMAND_LINE:
            os.system(f"gsutil -m rm -f gs://{self.bucket_name}/{remote_file}")
            print(f" --> [刪除檔案] \n --> 遠端檔案：{remote_file} \n")

    def set_bucket(self, new_bucket_name):
        """ 設定成新的 bucket """
        self.bucket_name = new_bucket_name
        self.bucket = self.client.bucket(self.bucket_name)
        print(f"GCS Bucket changed to {self.bucket_name}")

    def print_GCS_info(self):
        """ 印出 GCS 資訊 """
        print(" --> Google Cloud Storage 資訊：")
        print(f" ＊ Bucket name: {self.bucket_name}")
        print(f" ＊ Client: {self.client}")
        print(f" ＊ Bucket: {self.bucket}")

    def get_bucket(self):
        """ 取得 bucket """
        return self.bucket
    
    def get_client(self):
        """ 取得 client """
        return self.client

if __name__ == "__main__":
    help(GCS_Manager)