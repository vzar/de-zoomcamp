from pathlib import Path
import pandas as pd
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
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    # check if the file is already there and download if it's not
    os.system(f"test -f data/{dataset_file}.csv.gz || wget -O data/{dataset_file}.csv.gz {dataset_url}")
    path = Path(f"data/{dataset_file}.parquet")
    # only read header to get dtypes 
    dtypes=pd.read_csv(f"data/{dataset_file}.csv.gz",nrows=0).dtypes
    # setting dtype dictionary for parsing
    n_dtypes={}
    for k in dtypes.keys(): n_dtypes[k]=str
    # Int64 for nullable integer columns
    n_dtypes["PUlocationID"]=pd.Int64Dtype()
    n_dtypes["DOlocationID"]=pd.Int64Dtype()
    n_dtypes["SR_Flag"]=pd.Int64Dtype()
    # read csv and save to parquet, use parse_date to convert to datetime type
    pd.read_csv(f"data/{dataset_file}.csv.gz",dtype=n_dtypes,parse_dates=['pickup_datetime','dropOff_datetime']).\
       to_parquet(path, compression="gzip")
    write_gcs(path)


if __name__ == "__main__":
    for month in range(1,13): etl_web_to_gcs("fhv",2019,month)
