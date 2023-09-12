import os
import boto3
from botocore.exceptions import ClientError
import tarfile
import shutil
import json
from dateutil.relativedelta import relativedelta
import datetime
import zipfile
import csv
import pandas as pd
import psycopg2
from io import StringIO
import time

class S3Processor:
    def __init__(self, source_bucket_name: str, source_prefix: str, target_bucket_name: str,
                 target_prefix: str, backup_bucket_name: str, backup_prefix: str) -> None:
        self.s3 = boto3.resource('s3')
        self.source_bucket_name = source_bucket_name
        self.source_prefix = source_prefix
        self.target_bucket_name = target_bucket_name
        self.target_prefix = target_prefix
        self.backup_bucket_name = backup_bucket_name
        self.backup_prefix = backup_prefix
        self.downloaded_xxxx_files_path = []
        # クエリ実行時間計測用
        self.total_copy_time = 0

    def download_xxxx_files(self) -> None:
        source_bucket = self.s3.Bucket(self.source_bucket_name)
        for obj in source_bucket.objects.filter(Prefix=self.source_prefix):
            if obj.key.endswith('.tar.gz'):
                local_file_path = self._download_file_gluetemp_dir(self.source_bucket_name, obj.key)
                self.downloaded_xxxx_files_path.append(local_file_path)

    def process_files(self) -> None:
        secret_name = "XXXX_XXXX"
        secret_json = self._get_secret(secret_name, region_name="ap-northeast-1")

        for tar_file_path in self.downloaded_xxxx_files_path:
            extract_dir = self._extract_tar_file(tar_file_path)
            for filename in os.listdir(extract_dir):
                if filename.lower().endswith('.csv'):
                    local_csv_path = f'{extract_dir}/{filename}'
                    self._process_csv(local_csv_path)
                    s3_path = f'{self.target_prefix}{filename}'
                    self._upload_file_to_s3(local_csv_path, s3_path)
                    self._copy_to_redshift(f's3://{self.target_bucket_name}/{s3_path}', secret_json)
                    os.remove(local_csv_path)

            os.remove(tar_file_path)
            shutil.rmtree(extract_dir)

    def _download_file_gluetemp_dir(self, bucket_name: str, key: str) -> json:
        local_file_path = f'/tmp/{os.path.basename(key)}'
        self.s3.meta.client.download_file(bucket_name, key, local_file_path)
        return local_file_path

    def _process_csv(self, file_path):
        df = pd.read_csv(file_path, skiprows=4, encoding='Shift_JIS')
        df['datetime'] = pd.to_datetime(df.iloc[:, 0:5].astype(str).agg('-'.join, axis=1), format='%Y-%m-%d-%H-%M')
        df['datetime'] = df['datetime'].dt.floor('min')  # datetimeを分単位に丸める
        df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')  # datetimeを文字列に変換する
        df['pickup_colomn'] = df[['hoge', 'huga']].max(axis=1)

        mapping = {
            "datetime": "datetime",
            "huga": "piyo",

        }
        df.rename(columns=mapping, inplace=True)
        df = df[list(mapping.values())]
        df.to_csv(file_path, index=False)

    def _get_secret(self, secret_name: str, region_name: str) -> json:
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            # For a list of exceptions thrown, see
            # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            print(f"Failed to retrieve secret: {secret_name}. Error: {e}")
            raise e

        # Decrypts secret using the associated KMS key.
        return json.loads(get_secret_value_response['SecretString'])

    def _extract_tar_file(self, tar_file_path: str) -> str:
        extract_dir = f'/tmp/{os.path.splitext(os.path.basename(tar_file_path))[0]}'
        os.makedirs(extract_dir, exist_ok=True)
        with tarfile.open(tar_file_path, 'r:gz') as tar:
            tar.extractall(path=extract_dir)
        return extract_dir

    def _upload_file_to_s3(self, local_path: str, s3_path: str) -> None:
        self.s3.meta.client.upload_file(local_path, self.target_bucket_name, s3_path)

    def _copy_to_redshift(self, s3_path: str, secret_json: json) -> None:
        start_time = time.time()

        conn = psycopg2.connect(
            host=secret_json['host'],
            port=secret_json['port'],
            database=secret_json['database'],
            user=secret_json['user'],
            password=secret_json['password']
        )

        cur = conn.cursor()
        sql = f"""
            COPY xxxx
            FROM '{s3_path}'
            IAM_ROLE 'arn:aws:iam::XXXXXXXX:role/AWSGlueServiceRoleDefault'
            CSV
            DELIMITER ','
            IGNOREHEADER 1
            DATEFORMAT 'YYYY-MM-DD HH:MI:SS'
            TIMEFORMAT 'auto';
        """

        # executeが失敗した時のテーブル名とhostを表示させたほうがよい
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()

        end_time = time.time()
        self.total_copy_time += end_time - start_time

    def _add_file_to_zip(self, key: str, zip_file: zipfile.ZipFile) -> None:
        local_file_path = self._download_file_glue_tempdirectory(self.target_bucket_name, key)
        zip_file.write(local_file_path, arcname=os.path.basename(local_file_path))
        os.remove(local_file_path)

    # """
    # ターゲットバケットにアップロードしたCSVファイルを.zipファイルにしてバックアップバケットにアップロードする
    # """
    def backup_files(self) -> None:
        now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        with zipfile.ZipFile(f'/tmp/{now}_xxxx_xxx_xxxx.zip', 'w') as new_zip:
            target_bucket = self.s3.Bucket(self.target_bucket_name)
            for obj in target_bucket.objects.filter(Prefix=self.target_prefix):
                if obj.key.lower().endswith('.csv'):
                    self._add_file_to_zip(obj.key, new_zip)

        self._upload_file_to_s3(f'/tmp/{now}_xxxx_xxx_xxxx.zip', f'{self.backup_prefix}{now}_xxxx_xxx_xxxx.zip')
        os.remove(f'/tmp/{now}_xxxx_xxx_xxxx.zip')

    def delete_files_in_target_bucket(self) -> None:
        target_bucket = self.s3.Bucket(self.target_bucket_name)
        for obj in target_bucket.objects.filter(Prefix=self.target_prefix):
            self.s3.Object(self.target_bucket_name, obj.key).delete()

if __name__ == '__main__':
    past_date = datetime.datetime.now() - relativedelta(months=7)

    processor = S3Processor(
        source_bucket_name='xxxx',
        # # 取得先のS3バケットを動的に変える
        source_prefix = f"xxxx/xxxx/xxxx_{past_date.strftime('%Y%m%d')}/",
        target_bucket_name='xxxx',
        target_prefix='xxx/temp/',
        backup_bucket_name='xxxx',
        backup_prefix='bk/'
    )

    try:
        start_time_process = time.time()
        processor.download_xxxx_files()
        processor.process_files()
        end_time = time.time()
        # ファイル処理 実行時間の計算
        execution_time_process = end_time - start_time_process
        print(f"Time taken to download and process files is {execution_time_process} seconds")
        print(f"Total time spent on copying files to Redshift is {processor.total_copy_time} seconds")
        processor.backup_files()
    except Exception as e:
        print(f"An error occurred during processing: {e}")
    finally:
        start_time_delete = time.time()
        processor.delete_files_in_target_bucket()
        end_time = time.time()
        # ファイル削除 実行時間の計算2/2
        execution_time_delete = end_time - start_time_delete
        print(f"delete files in target bucket time is {execution_time_delete} seconds.")
