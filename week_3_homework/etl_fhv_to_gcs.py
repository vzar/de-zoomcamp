from pathlib import Path
#import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os



@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """The main ETL function"""
    #color = "green"
    #year = 2020
    #month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    os.system(f"test -f data/{dataset_file}.csv.gz || wget -O data/{dataset_file}.csv.gz {dataset_url}")
    #path = write_local(df_clean, color, dataset_file)
    path = Path(f"data/{dataset_file}.csv.gz")
    write_gcs(path)


if __name__ == "__main__":
    for month in range(1,13): etl_web_to_gcs("fhv",2019,month)
