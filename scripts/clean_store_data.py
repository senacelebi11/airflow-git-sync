import pandas as pd
import os

# MinIO içindeki CSV dosyasının yolu
input_file_path = "/dataops/raw/dirty_store_transactions.csv"
output_file_path = "/dataops/clean/clean_data_transactions.csv"

df = pd.read_csv(input_file_path)

df = df.drop_duplicates()

df = df.dropna()

os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
df.to_csv(output_file_path, index=False)
print("Cleaning completed and saved to:", output_file_path)
