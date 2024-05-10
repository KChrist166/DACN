import pandas as pd
import os
import numpy as np

# Đường dẫn đến thư mục chứa các file
csv_folder_path = "D:/UIT/KLTN/train_csv/"
img_folder_path = "D:/UIT/KLTN/train_img/"
rain_data_path = "D:/UIT/KLTN/data_rain.csv"
image_data_path = "D:/UIT/KLTN/data_img.csv"
final_data_path = "D:/UIT/KLTN/main_data.csv"
threshold_size = 100 * 1024  # Ví dụ: 100KB

def find_img_folder_matched(csv_name):
    for img_folder in os.listdir(img_folder_path):
        full_img_folder_path = os.path.join(img_folder_path, img_folder)   
        if os.path.isdir(full_img_folder_path) and img_folder == csv_name:
            matching_img_folder = full_img_folder_path
            break
    return matching_img_folder

def get_csv_file_name(file_path):
    csv_file = os.path.basename(file_path)
    csv_name = csv_file[:-4]
    return csv_name

def remove_invalid_paths(df):
    rows_to_remove = []
    for index, row in df.iterrows():
        filepath = row["Path"]
        if not os.path.isfile(filepath):
            rows_to_remove.append(index)
    df_cleaned = df.drop(rows_to_remove)
    return df_cleaned
        
def filter_and_delete_white_images(image_folder, threshold_size):
    white_images = []
    for filename in os.listdir(image_folder):
        filepath = os.path.join(image_folder, filename)
        if os.path.isfile(filepath):
            size = os.path.getsize(filepath)  # Lấy kích thước file
            if size <= threshold_size:  # Kiểm tra kích thước
                white_images.append(filepath)
    for image in white_images:
        os.remove(image)

# ----------------------------------------------------------------------------------------------------
# Đọc tất cả các file CSV và ghép chúng lại thành một DataFrame duy nhất
dfs = []
for csv_file in os.listdir(csv_folder_path):
    csv_file_path = os.path.join(csv_folder_path, csv_file)
    csv_name = get_csv_file_name(csv_file_path)
    if (find_img_folder_matched(csv_name)):
        df = pd.read_csv(csv_file_path) 
        time_value = csv_file.split('.')[0]
        # Thêm cột "Time"
        df['Time'] = time_value
        dfs.append(df)

# Ghép tất cả các DataFrame thành một DataFrame duy nhất
rain_df = pd.concat(dfs, ignore_index=True)
rain_df.to_csv(rain_data_path, index=False)

# ----------------------------------------------------------------------------------------------------

data = []

# Lặp qua tất cả các thư mục trong thư mục cơ sở
for folder_name in os.listdir(img_folder_path):
    folder_path = os.path.join(img_folder_path, folder_name)
    # Kiểm tra xem đối tượng có phải là thư mục không
    if os.path.isdir(folder_path):
        # Lặp qua tất cả các tập tin trong thư mục
        for file_name in os.listdir(folder_path):
            # Kiểm tra xem tập tin có phải là tập tin ảnh không
            if file_name.endswith('.png'):
                # Tạo đường dẫn tới tập tin ảnh
                image_path = os.path.join(folder_path, file_name)
                # Thêm đường dẫn và thời gian vào danh sách
                data.append({'Path': image_path, 'Time': folder_name, 'City': file_name})
        df        
# Tạo DataFrame từ danh sách dữ liệu                
img_df = pd.DataFrame(data)
img_df['City'] = img_df['City'].str.replace(".png", "")
img_df['Path'] = img_df['Path'].str.replace("\\", "/")

# Lưu DataFrame thành tập tin CSV
img_df.to_csv(image_data_path, index=False)

# ----------------------------------------------------------------------------------------------------
# Merge data
# Đọc hai file CSV
data_rain = pd.read_csv(rain_data_path)
data_img = pd.read_csv(image_data_path)

# Gộp hai DataFrame dựa trên cột "Time"
combined_df = pd.merge(data_rain, data_img, on=["Time", "City"], how="inner")

# Lựa chọn các cột bạn muốn giữ lại
combined_df = combined_df[["Path", "Rain (mm)", "Time", "City"]]

# ----------------------------------------------------------------------------------------------------
# Xoá ảnh lỗi 
for folder in os.listdir(img_folder_path):
    path = os.path.join(img_folder_path, folder)
    filter_and_delete_white_images(path, threshold_size)

# ----------------------------------------------------------------------------------------------------
# Lọc lại dữ liệu trong csv
data_cleaned_df = remove_invalid_paths(combined_df)
data_cleaned_df.to_csv(final_data_path, index=False)
