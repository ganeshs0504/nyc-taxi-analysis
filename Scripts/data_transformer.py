import pandas as pd
import requests
import io
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(hv_trips, *args, **kwargs):

    lookup_url = 'https://storage.googleapis.com/nyc-taxi-analysis-bucket/taxi_zone_lookup.csv'
    response = requests.get(lookup_url)
    location_dim = pd.read_csv(io.StringIO(response.text), sep=',')
    
    hv_trips = hv_trips.drop_duplicates().reset_index(drop=True)
    hv_trips['trip_id'] = hv_trips.index

    license_data = {
        'hvfhs_license_num': ['HV0002', 'HV0003', 'HV0004', 'HV0005'],
        'license_name': ['juno', 'uber', 'via', 'lyft']
    }
    license_dim = pd.DataFrame(license_data)

    unique_datetimes = pd.Series(pd.unique(hv_trips[['request_datetime', 'on_scene_datetime', 'pickup_datetime', 'dropoff_datetime']].values.ravel('K')))
    unique_datetimes = unique_datetimes.dropna()

    datetime_dim = pd.DataFrame(data=unique_datetimes, columns=['datetime'])
    datetime_dim['year'] = datetime_dim['datetime'].dt.year
    datetime_dim['month'] = datetime_dim['datetime'].dt.month
    datetime_dim['day'] = datetime_dim['datetime'].dt.day
    datetime_dim['hour'] = datetime_dim['datetime'].dt.hour
    datetime_dim['minute'] = datetime_dim['datetime'].dt.minute
    datetime_dim['day_of_week'] = datetime_dim['datetime'].dt.day_name()
    datetime_dim['is_weekend'] = datetime_dim['datetime'].dt.weekday >= 5

    fact_table = hv_trips[[
    'trip_id', 
    'hvfhs_license_num', 
    'request_datetime', 
    'on_scene_datetime', 
    'pickup_datetime', 
    'dropoff_datetime', 
    'PULocationID',
    'DOLocationID',
    'trip_miles',
    'trip_time',
    'base_passenger_fare',
    'tolls',
    'bcf',
    'sales_tax',
    'congestion_surcharge',
    'airport_fee',
    'tips',
    'driver_pay',
    'wav_request_flag',
    'wav_match_flag',
    ]]

    return {
        'license_dim': license_dim.to_dict(),
        'location_dim': location_dim.to_dict(),
        'datetime_dim': datetime_dim.to_dict(),
        'fact_table': fact_table.to_dict()
    }


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
