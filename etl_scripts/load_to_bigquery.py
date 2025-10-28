from google.cloud import bigquery
import pandas as pd
import os

# -------------------------------
# 1️⃣ Setup GCP Configuration
# -------------------------------
project_id = "your-gcp-project-id"        # replace with your actual GCP project ID
dataset_id = "retail_dataset"
data_folder = "data"

# Initialize BigQuery client
client = bigquery.Client(project=project_id)

# -------------------------------
# 2️⃣ Create dataset (if not exists)
# -------------------------------
dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
dataset_ref.location = "US"

try:
    client.get_dataset(dataset_ref)
    print(f"✅ Dataset {dataset_id} already exists.")
except Exception:
    client.create_dataset(dataset_ref)
    print(f"🆕 Created dataset {dataset_id}.")

# -------------------------------
# 3️⃣ Define CSV files and target tables
# -------------------------------
tables = {
    "orders": "orders_sample.csv",
    "customers": "customers_sample.csv",
    "products": "products_sample.csv"
}

# -------------------------------
# 4️⃣ Loop through files and upload
# -------------------------------
for table_name, csv_file in tables.items():
    file_path = os.path.join(data_folder, csv_file)
    df = pd.read_csv(file_path)

    table_ref = client.dataset(dataset_id).table(table_name)
    
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Wait for job to complete
    print(f"✅ Loaded {table_name} into BigQuery ({len(df)} rows).")

print("🎯 Data successfully loaded into BigQuery!")

