import pyarrow.parquet as pq
import pandas as pd
import requests
import io
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = 'https://storage.googleapis.com/nyc-taxi-analysis-bucket/fhvhv_tripdata_2023-01.parquet'
    response = requests.get(url)
    data = pq.read_table(io.BytesIO(response.content)).to_pandas()

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
