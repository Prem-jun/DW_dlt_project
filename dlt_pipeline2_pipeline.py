import dlt
import pandas as pd

data_folder = './data/users.csv'
df = pd.read_csv(data_folder)
data  = df.to_dict(orient='records')
 
pipeline = dlt.pipeline(
    pipeline_name="dlt_pipeline2", destination="duckdb", dataset_name="users"
)
load_info = pipeline.run(data, table_name="users")