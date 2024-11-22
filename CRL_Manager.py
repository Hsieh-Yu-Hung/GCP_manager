from GCS_Manager import GCS_Manager
from dataclasses import dataclass
from typing import List
import json
import os
import re

@dataclass
class FILETYPE:
    FOLDER = "folder"
    FILE = "file"

@dataclass
class TRANSFER_METHOD:
    CLI = "command_line"
    API = "python"

@dataclass
class FILES_IO:
    # Remote
    gcs_bucket: str
    gcs_path: str
    # Local
    local_path: str
    # Type
    file_type: FILETYPE
    # Transfer method
    transfer_method: TRANSFER_METHOD

@dataclass
class JOB_CONFIG:
    inputs: List[FILES_IO]
    outputs: List[FILES_IO]
    cli_params: List[str]

class CRL_Manager():

    """
    
        ＊ 在 CLI Container 執行模組, 下載資料 -> 執行 CLI -> 上傳結果

        ＊ 只要提供 CLI 指令的腳本和參數, 直接製作成容器部署在 Cloud Run 上

        ＊ CLI 腳本參數只能按照順序輸入, 請檢查 sh 腳本內容！

        usage:
            1. 建立 JOB_Config.conf 檔案, 準備好 cli 腳本和參數
            2. 在 JOB_Config.conf 中設定 inputs, outputs, cli_script_path, cli_params
            3. 建立實體 crl_manager = CRL_Manager("JOB_Config.conf")
            4. 執行 crl_manager.process_data(), 依照 config 的設定下載資料 -> 執行 CLI -> 上傳結果
    """

    def __init__(self, job_config_data: str, script_content: str):
        """ 建構式 """

        # Parse job config data from JSON string
        job_config_data = json.loads(job_config_data)

        # Convert job config data to JOB_CONFIG dataclass
        self.job_config = JOB_CONFIG(
            inputs=[FILES_IO(**input_file) for input_file in job_config_data["inputs"]],
            outputs=[FILES_IO(**output_file) for output_file in job_config_data["outputs"]],
            cli_params=job_config_data["cli_params"]
        )

        # Handle input and output files
        self.input_files = self.job_config.inputs
        self.output_files = self.job_config.outputs

        # CLI script content
        self.cli_script_content = script_content

        # CLI params
        self.cli_params = self.job_config.cli_params

    def download_data(self, input_files: List[FILES_IO]):
        """ 下載資料 """
        print(f" --> 下載資料...{len(input_files)} 個")
        try:
            for input_file in input_files:
                gcs_manager = GCS_Manager(input_file.gcs_bucket)
                # 如果是資料夾, 下載資料夾
                if input_file.file_type == FILETYPE.FOLDER:
                    gcs_manager.download_folder(input_file.gcs_path, input_file.local_path, mode=input_file.transfer_method)
                # 如果是檔案, 下載檔案
                elif input_file.file_type == FILETYPE.FILE:
                    gcs_manager.download_file(input_file.gcs_path, input_file.local_path, mode=input_file.transfer_method)
            return True
        except Exception as e:
            print(f"下載資料失敗: {e}")
            return False
    
    def parse_cli_script(self, cli_script_content: str):
        """ 解析 CLI 腳本 """
        print(" --> 解析 CLI 腳本...")

        # 找出有幾個參數(使用正則表達式找出所有以 $數字 結尾的行)
        param_count = 0
        for line in cli_script_content.split('\n'):
            matches = re.findall(r'\$\d+$', line)
            if matches:
                param_count = max(param_count, int(matches[0][1:]))

        # 檢查 CLI 腳本參數數量是否正確
        if param_count != len(self.cli_params):
            raise ValueError(f"CLI 腳本參數數量不正確, 應為 {param_count}, 但提供 {len(self.cli_params)} 個參數")
        
        # 將 CLI 腳本中的參數替換成實際的參數
        for i in range(param_count):
            cli_script_content = cli_script_content.replace(f"${i+1}", self.cli_params[i])
        
        return cli_script_content
        
    def execute_CLI(self, cli_commands: str):
        """ 執行 CLI 指令 """
        print(" --> 執行 CLI 指令...")
        try:
            os.system(cli_commands)
            return True
        except Exception as e:
            print(f"執行 CLI 指令失敗: {e}")
            return False

    def upload_results(self, output_files: List[FILES_IO]):
        """ 上傳結果 """
        print(" --> 上傳結果...")
        try:
            for output_file in output_files:
                gcs_manager = GCS_Manager(output_file.gcs_bucket)
                # 如果是資料夾, 上傳資料夾
                if output_file.file_type == FILETYPE.FOLDER:
                    gcs_manager.upload_folder(output_file.local_path, output_file.gcs_path, mode=output_file.transfer_method)
                # 如果是檔案, 上傳檔案
                elif output_file.file_type == FILETYPE.FILE:
                    gcs_manager.upload_file(output_file.local_path, output_file.gcs_path, mode=output_file.transfer_method)
            return True
        except Exception as e:
            print(f"上傳結果失敗: {e}")
            return False

    def process_data(self):
        """ 跑分析流程 """

        print(" --> 開始跑分析流程...")

        # step1: download data
        if not self.download_data(self.input_files):
            print("下載資料失敗, 分析流程結束")
            return

        # step2: parse CLI script
        cli_commands = self.parse_cli_script(self.cli_script_content)

        # step3: execute CLI
        if not self.execute_CLI(cli_commands):
            print("執行 CLI 指令失敗, 分析流程結束")
            return

        # step4: upload results
        if not self.upload_results(self.output_files):
            print("上傳結果失敗, 分析流程結束")
            return

        print(" --> 分析流程完成！")

if __name__ == "__main__":
    help(CRL_Manager)