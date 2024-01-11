import os
import io
import numpy as np
import pandas as pd
from PIL import Image
from sklearn.model_selection import train_test_split
import tensorflow as tf
from sklearn.preprocessing import StandardScaler
from keras.utils import to_categorical
from keras.applications.resnet50 import ResNet50
from keras import Sequential, layers, optimizers, Model
import base64
import csv

# Tạo Dataframe chứa thông tin mưa

base_path = "C:/Users/ASUS/Downloads/Compressed/KLTN"
csv_name = None
csv_folder_path = os.path.join(base_path, "Code_satelite_openweather")

def map_rain_value(rain_value):
    if rain_value != 0:
        return 1
    else:
        return 0

csv_data_df = pd.DataFrame()

for csv_file in os.listdir(csv_folder_path):
    if csv_file.endswith(".csv"):
        csv_file_path = os.path.join(csv_folder_path, csv_file)
        target_column_names = ["City", "Rain (mm)"]
        temp_df = pd.read_csv(csv_file_path)
        temp_df["Rain (mm)"] = temp_df["Rain (mm)"].apply(map_rain_value)
        unique_values = temp_df[target_column_names].drop_duplicates()
        temp_df_unique_values = pd.DataFrame(unique_values, columns=target_column_names)
        csv_file_name = os.path.basename(csv_file_path)
        csv_name = csv_file_name[:-4]
        print(f"Tên file CSV: {csv_file_name}\n")
        # print(temp_df_unique_values.to_string(index=False))
        csv_data_df = temp_df_unique_values

# print(csv_data_df.to_string(index=False))
print("Thời gian tạo file CSV:", csv_name)

# -----------------------------------------------------------------------------------------------------------------------------------

# Tìm folder ảnh có cùng thời gian với csv data

img_folder_path = os.path.join(base_path, "satelite_image_output_fixtime")

matching_img_folder = None

for img_folder in os.listdir(img_folder_path):
    full_img_folder_path = os.path.join(img_folder_path, img_folder)    # Tạo đường dẫn đầy đủ của thư mục ảnh
    
    # Kiểm tra xem thư mục có tồn tại không và có trùng với csv_file_name không
    if os.path.isdir(full_img_folder_path) and img_folder == csv_name:
        matching_img_folder = full_img_folder_path
        break   # Dừng vòng lặp nếu đã tìm thấy thư mục ảnh tương ứng

if matching_img_folder:
    print(f"Thư mục ảnh tương ứng với {csv_file_name} là: {matching_img_folder}")
    image_data = pd.DataFrame(columns=["Image"])    # Tạo DataFrame để lưu trữ thông tin về ảnh

    img_files = sorted(os.listdir(matching_img_folder), key=lambda x: x.split('.')[0])

    # Tạo list để lưu trữ các DataFrame tạm thời
    temp_dfs = []

    for img_file_name in img_files:
        img_file_path = os.path.join(matching_img_folder, img_file_name)
        with open(img_file_path, "rb") as img_file:
            img_data = base64.b64encode(img_file.read()).decode("utf-8")
        city = img_file_name.split('.')[0]  # Thêm dòng mới vào danh sách
        temp_df = pd.DataFrame({"City": [img_file_name[:-4]], "Image": [img_data]})     # Tạo DataFrame tạm thời để thêm dữ liệu mới
        temp_dfs.append(temp_df)     # Thêm DataFrame tạm thời vào list
        
    # Sử dụng pd.concat để ghép nối tất cả các DataFrame tạm thời vào DataFrame chính
    image_data = pd.concat([image_data] + temp_dfs, ignore_index=True)
    
    # print(image_data.to_string(index=False))      Hiển thị DataFrame mới mà không bao gồm cột index
    # print(image_data.head(10))
else:
    print(f"Không tìm thấy thư mục ảnh tương ứng với {csv_name}")

# -----------------------------------------------------------------------------------------------------------------------------------

# Ghép 2 dataframe lại với nhau

merged_data = pd.merge(image_data, csv_data_df, on='City')      # Merge hai DataFrame dựa trên cột 'City'
result_df = merged_data[['Image', 'Rain (mm)']]     # Chọn các cột quan trọng

for index, row in result_df.iterrows():
    final_result = pd.DataFrame(row).transpose()
    # print(final_result.head(5))
    # print("\n")
print(result_df)

# -----------------------------------------------------------------------------------------------------------------------------------

# Build model

image_for_reprocessing = result_df['Image']

images = [Image.open(io.BytesIO(base64.b64decode(img))) for img in result_df['Image']]      # Load ảnh từ cột 'Image'

resized_images = [img.resize((224, 224)) for img in images]     # Resize ảnh thành 224x224
X_resized = np.array([np.array(img) for img in resized_images])     # Chuyển ảnh thành mảng numpy
X_resized = X_resized / 255.0     # Chuẩn hoá giá trị pixel

# Reshape dữ liệu đầu vào để phù hợp với CNN
image_height, image_width = X[0].shape[0], X[0].shape[1]
num_channels = X[0].shape[2] if len(X[0].shape) == 3 else 1
X = X.reshape(-1, image_height, image_width, num_channels)

# Chuẩn bị các label
y = result_df['Rain (mm)']
y = to_categorical(y)

# Chia dữ liệu
X_train, X_test, y_train, y_test = train_test_split(X_resized, y, test_size=0.2, random_state=42)
# print(X.shape, X_train.shape, X_test.shape)

# Tạo model với ResNet50
convolutional_base = ResNet50(weights='imagenet', include_top=False, input_shape=(224, 224, 3))

model = Sequential()
# Add upsampling layers to match the input shape expected by ResNet50
model.add(layers.UpSampling2D((2, 2)))
model.add(layers.UpSampling2D((2, 2)))
model.add(layers.UpSampling2D((2, 2)))

model.add(convolutional_base)      # Add the ResNet50 convolutional base
model.add(layers.Flatten())     # Flatten the output of ResNet50
model.add(layers.BatchNormalization())      # Batch normalization layer
model.add(layers.Dense(128, activation='relu'))     # Dense layer with ReLU activation
model.add(layers.Dropout(0.5))      # Dropout layer to prevent overfitting
model.add(layers.BatchNormalization())      # Batch normalization layer

# Output layer with sigmoid activation (assuming binary classification)
model.add(layers.Dense(1, activation='sigmoid'))

model.compile(optimizer=optimizers.RMSprop(learning_rate=2e-5),     # Compile the model
              loss='binary_crossentropy',
              metrics=['accuracy'])

# Build the model
model.build(input_shape=(None, 256, 256, 3))

# Display the model summary
model.summary()

# Train the model
# history = model.fit(X_train, y_train, epochs=10, validation_split=0.2)

