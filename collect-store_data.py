from sentinelhub import CRS, BBox, MimeType, SentinelHubRequest, SHConfig, DataCollection
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from minio import Minio
from minio.error import S3Error
import shutil
import requests
import pandas as pd
import os
import csv

# Cài đặt cấu hình
CLIENT_ID = "7b96af74-8c01-41fb-b0c0-7759db46dcf8"
CLIENT_SECRET = "L8xDsH9gFc0JQk8SApWVW2PSObGecnpG"

config = SHConfig()
if CLIENT_ID and CLIENT_SECRET:
    config.sh_client_id = CLIENT_ID
    config.sh_client_secret = CLIENT_SECRET

# Danh sách tên tỉnh và tọa độ tương ứng
province_coordinates = [
    ("Ha Noi", 105.8412, 21.0245),
    ("Ho Chi Minh", 106.6667, 10.75),
    ("Hai Phong", 106.6822, 20.8561),
    ("Can Tho", 105.7833, 10.0333),
    ("Da Nang", 108.2208, 16.0678),
    ("Bien Hoa", 106.8167, 10.95),
    ("Hai Duong", 106.3167, 20.9333),
    ("Hue", 107.6, 16.4667),
    ("Binh Duong", 109.0797, 14.2972),
    ("Thu Duc", 106.7558, 10.8526),
    ("Ba Ria", 107.1688, 10.4963),
    ("Bac Lieu", 105.7244, 9.285),
    ("Da Lat", 108.4419, 11.9465),
    ("Bac Giang", 106.2, 21.2667),
    ("Bac Kan", 105.8333, 22.1333),
    ("Bac Ninh", 106.05, 21.1833),
    ("Ben Tre", 106.3833, 10.2333),
    ("Buon Ma Thuot", 108.05, 12.6667),
    ("Ca Mau", 105.15, 9.1769),
    ("Cam Ranh", 109.1591, 11.9214),
    ("Cao Bang", 106.25, 22.6667),
    ("Cao Lanh", 105.6333, 10.45),
    ("Cam Pha mines", 107.3, 21.0167),
    ("Chau Doc", 105.1167, 10.7),
    ("Dien Bien Phu", 103.0167, 21.3833),
    ("Dong Ha", 107.1003, 16.8163),
    ("Dong Hoi", 106.6, 17.4833),
    ("Dong Xoai", 106.9167, 11.5333),
    ("Ha Giang", 104.9833, 22.8333),
    ("Ha Long", 107.08, 20.9511),
    ("Ha Tien", 104.4833, 10.3833),
    ("Ha Tinh", 105.9, 18.3333),
    ("Hoa Binh", 105.3383, 20.8133),
    ("Hoi An", 108.335, 15.8794),
    ("Hung Yen", 106.0667, 20.65),
    ("Kon Tum", 108, 14.35),
    ("Lai Chau", 103.4517, 22.3997),
    ("Lang Son", 106.7333, 21.8333),
    ("Lao Cai", 103.95, 22.4833),
    ("Long Xuyen", 105.4167, 10.3833),
    ("My Tho", 106.35, 10.35),
    ("Nam Dinh", 106.1667, 20.4167),
    ("Nha Trang", 109.1833, 12.25),
    ("Ninh Binh", 105.975, 20.2539),
    ("Phan Rang - Thap Cham", 108.9833, 11.5667),
    ("Phan Thiet", 108.1, 10.9333),
    ("Phu Ly", 105.9139, 20.5411),
    ("Pleiku", 108, 13.9833),
    ("Quang Ngai", 108.8, 15.1167),
    ("Quy Nhon", 109.2333, 13.7667),
    ("Rach Gia", 105.0833, 10.0167),
    ("Sa Dec", 105.7667, 10.3),
    ("Soc Trang", 105.98, 9.6033),
    ("Son La", 103.9, 21.3167),
    ("Tam Ky", 108.4833, 15.5667),
    ("Tan An", 106.4167, 10.5333),
    ("Tay Ninh", 106.1, 11.3),
    ("Thai Binh", 106.3333, 20.45),
    ("Thai Nguyen", 105.8442, 21.5928),
    ("Thanh Hoa", 105.7667, 19.8),
    ("Thu Dau Mot", 106.65, 10.9667),
    ("Tra Vinh", 106.3453, 9.9347),
    ("Tuy Hoa", 109.3, 13.0833),
    ("Tuyen Quang", 105.2181, 21.8233),
    ("Uong Bi", 106.7833, 21.0333),
    ("Sa Pa", 103.8441, 22.3402),
    ("Viet Tri", 105.4308, 21.3019),
    ("Vinh", 105.6667, 18.6667),
    ("Vinh Long", 105.9667, 10.25),
    ("Vinh Yen", 105.5967, 21.31),
    ("Vung Tau", 107.0843, 10.346),
    ("Yen Bai", 104.8667, 21.7),
    ("Di An", 106.7668, 10.9129),
    ("Con Son", 106.6167, 8.6833),
    ("Soc Trang", 105.8264, 9.8107),
    ("Long Khanh", 107.2114, 10.9506),
]

# Tạo danh sách bbox
province_bboxes = {}

for province, lon, lat in province_coordinates:
    bbox = BBox((lon - 0.1, lat - 0.1, lon + 0.1, lat + 0.1), crs=CRS.WGS84)
    province_bboxes[province] = bbox

# Đặt kích thước đầu ra của ảnh
output_size = (224, 224)

# Thiết lập thời gian (lấy thời gian hiện tại trực tiếp)
current_time = datetime.now()
time_interval = (current_time - timedelta(days=30)).isoformat(), current_time.isoformat()

# Thiết lập màu ảnh RGB
evalscript_true_color = """
//VERSION=3

function setup() {
    return {
        input: [{
            bands: ["B04", "B03", "B02"]
        }],
        output: {
            bands: 3
        }
    };
}

function evaluatePixel(sample) {
    return [2.5 * sample.B04, 2.5 * sample.B03, 2.5 * sample.B02];
}
"""

# Lặp qua danh sách tỉnh thành và lưu ảnh vào ListImg
ListImg = []
for province, bbox in province_bboxes.items():
    request = SentinelHubRequest(
        evalscript=evalscript_true_color,
        input_data=[
            SentinelHubRequest.input_data(
            data_collection=DataCollection.SENTINEL2_L2A,
            time_interval=time_interval,
        )
    ],
    responses=[SentinelHubRequest.output_response("default", MimeType.PNG)],
    bbox=bbox,
    size=output_size,
    config=config)

    ListImg.append((province, request.get_data()[0]))
    

# Lấy thời gian trước khi bắt đầu vòng lặp
current_time_before_execution = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

# Định nghĩa thư mục lưu trữ hình ảnh
output_directory = '/usr/local/khanh/DACN/satelite_image_output_realtime'
os.makedirs(output_directory, exist_ok=True)

# Tạo một danh sách để lưu trữ thông tin về tất cả các ảnh
image_data = []

# Biến số đếm để theo dõi tổng số ảnh đã lưu trữ
total_images = 0
# current_time = str(datetime.datetime.now())
current_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

for prov, img in ListImg:
#   folder = os.path.join(output_directory, current_time)
    folder = os.path.join(output_directory, current_time_before_execution)
    if not os.path.exists(folder):
        os.mkdir(folder)
    output_path = os.path.join(folder, f'{prov}.png')

    plt.imshow(img)
    plt.axis('off')
    plt.savefig(output_path, bbox_inches='tight')

    # Thêm thông tin về hình ảnh và đường dẫn vào danh sách
    image_data.append((prov, output_path))
  
    total_images += 1

# In tổng số ảnh đã lưu trữ
print(f"Total images saved: {total_images}")

# Sau khi vòng lặp đã kết thúc, bạn có thể duyệt qua danh sách image_data để thực hiện bất kỳ xử lý nào khác.
for prov, path in image_data:
    print(f"Image saved for {prov} at {path}")

# Nén folder ảnh thành file zip cùng tên
folder_to_compress = os.path.join(output_directory, current_time_before_execution)
output_zip_path = os.path.join(output_directory, f"{current_time_before_execution}.zip")
shutil.make_archive(output_zip_path[:-4], 'zip', folder_to_compress)

# Hiện thông báo
print(f"\n{folder_to_compress} has already been zipped at the same directory\n")
print(f"{folder_to_compress} will be deleted now\n")

# Xoá folder ảnh
folder_to_delete = os.path.join(output_directory, current_time_before_execution)
try:
    shutil.rmtree(folder_to_delete)
    print(f"Folder '{folder_to_delete}' was deleted successfully.\n")
except OSError as e:
        print(f"Error: {e}")

# Upload Object lên Cloud
minio_server = "clouds.iec-uit.com"
access_key = "SFB2O8gfSOEcmwlIgy5I"
secret_key = "x0dXE4LQwOenRWe9rFyzj5EgtohAEb3buai5ijw1"
bucket_satellite_img_name = "iot-08.weather-satellite-realtime-dataset"
zipped_bucket_object_name = f"{current_time_before_execution}.zip"
local_zip_file_path = output_zip_path    # Change your local file path

try:
    client = Minio(
        minio_server,
        access_key = access_key,
        secret_key = secret_key,
        secure = False
    )
    # Upload './test.zip' as object name 'test.zip' to bucket 'weather-radar-dataset'.
    client.fput_object(
        bucket_satellite_img_name, zipped_bucket_object_name, local_zip_file_path
    )
    print(f"Data is successfully uploaded as object data to bucket {bucket_satellite_img_name}.\n"
    )
except S3Error as exc:
    print("error occurred.", exc)

# Xoá file zip
try:
    os.remove(output_zip_path)
    print(f"File '{output_zip_path}' has been deleted successfully.\n")
except OSError as e:
    print(f"Error: {e}")


# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

api_key = "80a3e60518b8b856cbb74d08f7da7007"

def get_weather_data(lat, lon, api_key):
    base_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
    response = requests.get(base_url)
    data = response.json()
    return data

def process_weather_data(data):
    city_name = data["name"] if "name" in data else "Unknown"
    
    weather_description = data["weather"][0]["description"] if "weather" in data else "N/A"
    
    rain_volume = data["rain"]["1h"] if "rain" in data and "1h" in data["rain"] else 0
    
    return city_name, weather_description, rain_volume

def save_to_csv(file_name, data):
    with open(file_name, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ["City", "Weather", "Rain (mm)"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for row in data:
            writer.writerow({
                "City": row[0],
                "Weather": row[1],
                "Rain (mm)": row[2]
            })

weather_data_list = []

for province in province_coordinates:
    data = get_weather_data(province[2], province[1], api_key)
    processed_data = process_weather_data(data)
    weather_data_list.append(processed_data)

output_csv_directory = '/usr/local/khanh/DACN/rainfall_data'
os.makedirs(output_csv_directory, exist_ok=True)
    
output_file_name = os.path.join(output_csv_directory, f"{current_time_before_execution}.csv")
    
save_to_csv(output_file_name, weather_data_list)

print(f"Data has been saved to {output_file_name}\n")
    
# Đọc file CSV vào DataFrame
csv_file_path = os.path.join(output_csv_directory, f"{current_time_before_execution}.csv")
df = pd.read_csv(csv_file_path)

city_updates = [
    (0, 'Ha Noi'),
    (1, 'Ho Chi Minh'),
    (2, 'Hai Phong'),
    (4, 'Da Nang'),
    (8, 'Binh Duong'),
    (9, 'Thu Duc'),
    (10, 'Ba Ria'),
    (22, 'Cam Pha mines'),
    (23, 'Chau Doc'),
    (26, 'Dong Hoi'),
    (27, 'Dong Xoai'),
    (36, 'Lai Chau'),
    (42, 'Nha Trang'),
    (44, 'Phan Rang - Thap Cham'),
    (49, 'Quy Nhon'),
    (64, 'Uong Bi'),
    (67, 'Vinh'),
    (72, 'Di An'),
    (74, 'Soc Trang'),
    (75, 'Long Khanh'),
]

for index, new_value in city_updates:
    df.at[index, 'City'] = new_value

# Lưu DataFrame với các chỉnh sửa vào file mới
df.to_csv(output_file_name, index=False, mode='w')

# Upload file csv lên Cloud
csv_bucket_name = "iot-08.rainfall-dataset"
csv_bucket_object_name = f"{current_time_before_execution}.csv"

try:
    client = Minio(
        minio_server,
        access_key = access_key,
        secret_key = secret_key,
        secure = False
    )
    
    client.fput_object(
        csv_bucket_name, csv_bucket_object_name, csv_file_path
    )
    print(f"Data is successfully uploaded as object data to bucket {csv_bucket_name}.\n"
    )
except S3Error as exc:
    print("error occurred.", exc)
    
# Xoá file csv
try:
    os.remove(csv_file_path)
    print(f"File '{csv_file_path}' has been deleted successfully.\n")
except OSError as e:
    print(f"Error: {e}")
