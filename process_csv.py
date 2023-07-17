import os
import csv
import shutil
import datetime
import random
import tarfile

# CSVファイルのコピー元(source_dir)とコピー先(target_dir)のディレクトリ、圧縮ファイルアップロード先(upload_dir)を指定
source_dir = "source"
target_dir = "./"
upload_dir = "./upload/"

# 処理開始前の整合性を保つためにコピー先ディレクトリ内のファイルを削除
def remove_files_starting_with_2023(target_dir):
    for filename in os.listdir(target_dir):
        if filename.lower().startswith('2023') and filename.lower().endswith('.csv'):
            os.remove(os.path.join(target_dir, filename))

def copy_all_csv_files(source_dir, target_dir):
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            if file.lower().endswith('.csv'):
                source_file_path = os.path.join(root, file)
                target_file_path = os.path.join(target_dir, file)
                shutil.copy2(source_file_path, target_file_path)

def skip_and_merge_rows(csv_path):
    # 'Shift_JIS'エンコーディングでファイルの行を読み込む、エラーは無視
    with open(csv_path, 'r', encoding='Shift_JIS', errors='ignore') as file:
        reader = csv.reader(file)
        lines = list(reader)

    # ランダムパターンを定義
    random_pattern = {
    }

    new_lines = []
    for i, line in enumerate(lines):
        # 1-4行目はそのまま出力(ヘッダー箇所)
        if i < 5:
            new_lines.append(line)
        else:
            try:
                for j in range(1, 2):
                    # 新しい日付時間文字列+IP箇所のランダムパターンで新しいレコードを作成
                    new_line = line[0:0] + random.choice(list(random_pattern.values()))
                    new_lines.append(new_line)
            except ValueError:
                new_lines.append(line)

    # エンコーディングで行を書き出し
    with open(csv_path, 'w', encoding='Shift_JIS', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(new_lines)

def compress_file(file_path, output_dir):
    # tar.gz形式でファイルを圧縮
    output_file_path = os.path.join(output_dir, f'{os.path.basename(file_path)}.tar.gz')
    with tarfile.open(output_file_path, 'w:gz', format=tarfile.GNU_FORMAT) as tar:
        tar.add(file_path, arcname=os.path.basename(file_path))

# スクリプト実行前に'2023'で始まるCSVファイルを削除
remove_files_starting_with_2023(target_dir)
# source_dir内の全てのCSVファイルをtarget_dirにコピー
copy_all_csv_files(source_dir, target_dir)

# target_dir内の全てのCSVファイルを処理する
for filename in os.listdir(target_dir):
    if filename.lower().endswith('.csv'):
        print(f"Processing file: {filename}")
        try:
            csv_path = os.path.join(target_dir, filename)
            skip_and_merge_rows(csv_path)
            print(f"Successfully processed file: {filename}")
            # 処理が成功したらファイルを圧縮
            compress_file(csv_path, upload_dir)
            print(f"Successfully compressed file: {filename}")
        except Exception as e:
            print(f"Failed to process file: {filename}. Reason: {e}")
