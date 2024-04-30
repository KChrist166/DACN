import pandas as pd
import os
import numpy as np

img_folder = ""

# Đường dẫn đến thư mục chứa các file CSV
folder_path = "D:/UIT/KLTN/train_csv/"
base_folder_path = "D:/UIT/KLTN/train_img/"

# ----------------------------------------------------------------------------------------------------

# Danh sách các file CSV trong thư mục
csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]

# Đọc tất cả các file CSV và ghép chúng lại thành một DataFrame duy nhất
dfs = []
for csv_file in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    df = pd.read_csv(file_path) 
    time_value = csv_file.split('.')[0]
    # Thêm cột "Time"
    df['Time'] = time_value
    dfs.append(df)

# Ghép tất cả các DataFrame thành một DataFrame duy nhất
combined_df = pd.concat(dfs, ignore_index=True)

# Lưu DataFrame kết hợp thành một file CSV
rain_csv_path = "D:/UIT/KLTN/data_rain.csv"
combined_df.to_csv(rain_csv_path, index=False)

# ----------------------------------------------------------------------------------------------------

data = []
# Lặp qua tất cả các thư mục trong thư mục cơ sở
for folder_name in os.listdir(base_folder_path):
    folder_path = os.path.join(base_folder_path, folder_name)
    # Kiểm tra xem đối tượng có phải là thư mục không
    if os.path.isdir(folder_path):
        # Lặp qua tất cả các tập tin trong thư mục
        for file_name in os.listdir(folder_path):
            # Kiểm tra xem tập tin có phải là tập tin ảnh không
            if file_name.endswith('.png'):
                # Tạo đường dẫn tới tập tin ảnh
                image_path = os.path.join(folder_path, file_name)
                # Thêm đường dẫn và thời gian vào danh sách
                data.append({'Path': image_path, 'Time': folder_name})

# Tạo DataFrame từ danh sách dữ liệu
df = pd.DataFrame(data)
df['Path'] = df['Path'].str.replace("\\", "/")

# Lưu DataFrame thành tập tin CSV
img_csv_path = "D:/UIT/KLTN/data_img.csv"
df.to_csv(img_csv_path, index=False)

# ----------------------------------------------------------------------------------------------------

data_rain = pd.read_csv("D:/UIT/KLTN/data_rain.csv")
data_img = pd.read_csv("D:/UIT/KLTN/data_img.csv")

# Gộp hai DataFrame dựa trên cột "Time"
data_combined = pd.merge(data_img, data_rain, on="Time")

# Lựa chọn các cột bạn muốn giữ lại
data_combined = data_combined[["Path", "Rain (mm)"]]

data_csv_path = "D:/UIT/KLTN/data.csv"
# Lưu DataFrame kết quả thành file CSV
data_combined.to_csv(data_csv_path, index=False)