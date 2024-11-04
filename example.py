"""

    這個腳本用來演示如何使用 GCS_Manager 來下載和上傳檔案

"""

from GCS_Manager import GCS_Manager
import os

# Bucket name
BUCKET_NAME = "accuinbio-core-dev"

# 建立 GCS 管理器
manager = GCS_Manager(bucket_name=BUCKET_NAME)

if __name__ == "__main__":

    # 印出 GCS 資訊
    manager.print_GCS_info()

    # 切換 Bucket
    #ANOTHER_BUCKET_NAME = "accuinbio-core-storage"
    #manager.set_bucket(new_bucket_name=ANOTHER_BUCKET_NAME)

    # 下載檔案
    GCS_PATH_FOR_DOWNLOAD = "annotation_db_outputs/Profile_2024-10-31"
    DOWNLOAD_TARGET_FILE = "accutate_db_profile.logs"
    LOCAL_PATH_FOR_DOWNLOAD = "./TEST_FILES"
    full_path_download_local_file = os.path.join(LOCAL_PATH_FOR_DOWNLOAD, DOWNLOAD_TARGET_FILE)
    full_path_download_remote_file = os.path.join(GCS_PATH_FOR_DOWNLOAD, DOWNLOAD_TARGET_FILE)
    
    """ 使用 python package 下載檔案 """
    #manager.download_file(remote_file=full_path_download_remote_file, local_file=full_path_download_local_file, mode='python')
    
    """ 使用 gsutil 下載檔案 """
    #manager.download_file(remote_file=full_path_download_remote_file, local_file=full_path_download_local_file, mode='command_line')

    # 上傳檔案
    GCS_PATH_FOR_UPLOAD = "TEST_UPLOAD"
    UPLOAD_TARGET_FILE = "HELLO.txt"
    LOCAL_PATH_FOR_UPLOAD = "./TEST_FILES"
    full_path_upload_local_file = os.path.join(LOCAL_PATH_FOR_UPLOAD, UPLOAD_TARGET_FILE)
    full_path_upload_remote_file = os.path.join(GCS_PATH_FOR_UPLOAD, UPLOAD_TARGET_FILE)
    
    """ 使用 python package 上傳檔案 """
    #manager.upload_file(local_file=full_path_upload_local_file, remote_file=full_path_upload_remote_file, mode='python')
    
    """ 使用 gsutil 上傳檔案 """
    #manager.upload_file(local_file=full_path_upload_local_file, remote_file=full_path_upload_remote_file, mode='command_line')

    # 上傳資料夾
    GCS_PATH_FOR_UPLOAD_FOLDER = "TEST_UPLOAD_FOLDER"
    LOCAL_PATH_FOR_UPLOAD_FOLDER = "./TEST_FILES"

    """ 使用 python package 上傳檔案 """
    #manager.upload_folder(local_folder=LOCAL_PATH_FOR_UPLOAD_FOLDER, remote_folder=GCS_PATH_FOR_UPLOAD_FOLDER, mode='python')

    """ 使用 gsutil 上傳檔案 """
    #manager.upload_folder(local_folder=LOCAL_PATH_FOR_UPLOAD_FOLDER, remote_folder=GCS_PATH_FOR_UPLOAD_FOLDER, mode='command_line')

    # 下載資料夾
    GCS_PATH_FOR_DOWNLOAD_FOLDER = "annotation_db_outputs/Profile_2024-11-01"
    LOCAL_PATH_FOR_DOWNLOAD_FOLDER = "./TEST_FILES"

    """ 使用 python package 下載檔案 """
    #manager.download_folder(remote_folder=GCS_PATH_FOR_DOWNLOAD_FOLDER, local_folder=LOCAL_PATH_FOR_DOWNLOAD_FOLDER, mode='python')

    """ 使用 gsutil 下載檔案 """
    #manager.download_folder(remote_folder=GCS_PATH_FOR_DOWNLOAD_FOLDER, local_folder=LOCAL_PATH_FOR_DOWNLOAD_FOLDER, mode='command_line')
