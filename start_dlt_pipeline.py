import dlt # type: ignore
import pandas as pd # type: ignore

data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
pipeline = dlt.pipeline(
    pipeline_name="start_dlt", destination="duckdb", dataset_name="adventurework2022"
)
load_info = pipeline.run(data, table_name="product")