import csv
import sys
import random
import string
import os

def get_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

def get_random_address():
    prefectures = ["北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県", "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県", "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県", "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県", "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県", "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"]
    city = get_random_string(5)
    return random.choice(prefectures) + " " + city + "市"


def get_random_ip():
    return ".".join(map(str, (random.randint(0, 255) for _ in range(4))))

def create_csv(filename):
    with open(filename, 'w') as csvfile:
        fieldnames = ['field1', 'field2', 'field3', 'field4', 'field5', 'field6', 'field7', 'field8', 'field9','field10']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        while os.path.getsize(filename) < 10 * 1024 * 1024:  # 10MB
            data = {
                'field1': get_random_string(5),
                'field2': get_random_ip(),
                'field3': get_random_string(3),
                'field4': get_random_string(4),
                'field5': get_random_string(5),
                'field6': get_random_string(6),
                'field7': get_random_string(7),
                'field8': get_random_string(8),
                'field9': get_random_string(9),
                'field10': get_random_string(10),

            }
            writer.writerow(data)

if __name__ == "__main__":
    filename = sys.argv[1]
    create_csv(filename)
