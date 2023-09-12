import boto3
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

def manage_trigger(i):
    target_job_name = f'd{i:02}_xxxx_xxxx_xxxx_xxxx'
    schedule_expression = "cron(40 2 * * ? *)"

    # トリガーの存在確認
    try:
        glue.get_trigger(Name=f"{target_job_name}_trigger")
        trigger_exists = True
    except Exception as e:
        print(f"Nothing trigger for {target_job_name}: {e}")
        trigger_exists = False

    # API実行スロットリング対策
    time.sleep(10)

    # トリガーが存在する場合、既存のトリガーを削除
    if trigger_exists:
        try:
            glue.delete_trigger(Name=f"{target_job_name}_trigger")
            # 削除完了前に作成するとエラーになるため待機
            time.sleep(40)
        except Exception as e:
            print(f"Error deleting trigger for {target_job_name}: {e}")

    # 新しいトリガーの作成
    # try:
    #     glue.create_trigger(
    #         Name=f"{target_job_name}_trigger",
    #         Type="SCHEDULED",
    #         Schedule=schedule_expression,
    #         Actions=[{"JobName": target_job_name}],
    #         StartOnCreation=True
    #     )
    # except Exception as e:
    #     print(f"Error creating trigger for {target_job_name}: {e}")


glue = boto3.client('glue')

# 実行開始時刻を表示
start_time = datetime.now()
print(f"Execution started at: {start_time}")

# GlueのAPI実行のスロットリング制限に抵触するため、max_workerを設定
# 処理中にsleepすることでも制限している
with ThreadPoolExecutor(max_workers=5) as executor:
    executor.map(manage_trigger, range(1, 101))

# 実行終了時刻と実行時間を表示
# 100ジョブ編集で17分弱かかる
end_time = datetime.now()
print(f"Execution ended at: {end_time}")
print(f"Total execution time: {end_time - start_time}")
