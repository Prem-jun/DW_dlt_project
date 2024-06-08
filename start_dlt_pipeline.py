import dlt # type: ignore
# import requests
import pandas as pd # type: ignore

data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
# data_load = './AdventureWork2022/Reseller.csv'
# df = pd.read_csv(data_load)
# data  = df.to_dict(orient="records")
pipeline = dlt.pipeline(
    pipeline_name="start_dlt", destination="duckdb", dataset_name="adventurework2022"
)
load_info = pipeline.run(data, table_name="product")