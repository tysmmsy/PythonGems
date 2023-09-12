import boto3
import botocore
import subprocess
import json
import os
import tempfile
import time

"""
処理ざっくり
source_job_nameをtarget_job_nameに置換する
old_string_1、old_string_2をscript_content処理で置換する
dXXとついた処理名、テーブル名に置換し、ジョブとテーブルを紐付けるようにしながら複製する
"""

# AWS Glueサービスクライアントの作成
glue = boto3.client('glue')
s3 = boto3.client('s3')

# 複製元のジョブ名
source_job_name = 'psjob_xxxx_xxxxxxxx'

# AWS CLIを使用してジョブの詳細を取得
cli_command = f"aws glue get-job --job-name {source_job_name}"
cli_output = subprocess.check_output(cli_command, shell=True)
job_details = json.loads(cli_output)

# ジョブの接続を取得
connections = job_details['Job']['Connections']['Connections']

# 複製元のジョブの詳細を取得
response = glue.get_job(JobName=source_job_name)
source_job = response['Job']

# 置換対象の文字列と置換後の文字列
old_string_1 = 'xxxx/poc/'
old_string_2 = 'COPY xxxx_xxxx_xxxx_xxxx'
old_string_3 = f'poc/temp/'

# スクリプトのダウンロード
source_script_url = source_job['Command']['ScriptLocation']
bucket_name = source_script_url.split('/')[2]
key = '/'.join(source_script_url.split('/')[3:])
local_file_path = os.path.join(tempfile.gettempdir(), os.path.basename(key))
s3.download_file(bucket_name, key, local_file_path)
with open(local_file_path, 'r') as file:
    original_script_content = file.read()

# 新しいジョブの作成
for i in range(1, 100):
    # 複製先のジョブ名
    target_job_name = f'd{i:02}_xxxx_xxxx_xxxx_xxxx'

    # 置換後の文字列
    new_string_1 = f'xxxx/poc/d{i:02}/'
    new_string_2 = f'COPY {target_job_name}'
    new_string_3 = f'poc/d{i:02}_temp/'

    # スクリプトの置換
    script_content = original_script_content.replace(old_string_1, new_string_1).replace(old_string_2, new_string_2).replace(old_string_3, new_string_3)
    with open(local_file_path, 'w') as file:
        file.write(script_content)
    target_script_key = key.replace('xxxx/poc/', new_string_1).replace(os.path.basename(key), f'{target_job_name}.py')
    s3.upload_file(local_file_path, bucket_name, target_script_key)
    target_script_url = f's3://{bucket_name}/{target_script_key}'

    # ジョブの存在確認
    try:
        glue.get_job(JobName=target_job_name)
        job_exists = True
    except Exception:
        job_exists = False

        # ジョブの作成または更新
    if job_exists:
        # ジョブの更新
        glue.update_job(
            JobName=target_job_name,
            JobUpdate={
                'Role': source_job['Role'],
                'Command': {
                    'Name': source_job['Command']['Name'],
                    'ScriptLocation': target_script_url,
                    'PythonVersion': source_job['Command'].get('PythonVersion')
                },
                'DefaultArguments': source_job['DefaultArguments'],
                'Connections': {
                    'Connections': connections
                },
                'MaxRetries': source_job['MaxRetries'],
                'Timeout': source_job['Timeout'],
                # 1 or 0.0625
                'MaxCapacity': 0.0625,
                'GlueVersion': source_job['GlueVersion']
            }
        )
    else:
        # 新しいジョブの作成
        glue.create_job(
            Name=target_job_name,
            Role=source_job['Role'],
            Command={
                'Name': source_job['Command']['Name'],
                'ScriptLocation': target_script_url,
                'PythonVersion': source_job['Command'].get('PythonVersion')
            },
            DefaultArguments=source_job['DefaultArguments'],
            Connections={
                'Connections': connections  # ここに接続のリストを設定
            },
            MaxRetries=source_job['MaxRetries'],
            Timeout=source_job['Timeout'],
            MaxCapacity=0.0625,
            GlueVersion=source_job['GlueVersion']
        )

