# load
import io
import pandas as pd
import requests
import time
from datetime import datetime, timezone

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    all_data = []
    crypto_ids = ['bitcoin', 'ethereum', 'tether', 'binancecoin', 'solana']
    
    for crypto in crypto_ids:
        url = f'https://api.coingecko.com/api/v3/coins/{crypto}/market_chart'
        params = {
            'vs_currency': 'usd',
            'days': 60,
            'interval': 'daily'
        }
        
        response = requests.get(url, params=params)
        time.sleep(5)
        
        if response.status_code == 200:
            data = response.json()
            prices = data['prices']
            market_caps = data['market_caps']
            volumes = data['total_volumes']
            
            date = [datetime.fromtimestamp(item[0] / 1000).strftime('%Y-%m-%d') for item in prices]
            price = [item[1] for item in prices]
            market_cap = [item[1] for item in market_caps]
            volume = [item[1] for item in volumes]

            crypto_df = pd.DataFrame({
                'date': date,
                'cryptocurrency': [crypto.capitalize()] * len(date),  # Powtórzenie wartości dla każdej daty
                'price_usd': price,
                'market_cap_usd': market_cap,
                'volume_usd': volume
            })
            all_data.append(crypto_df)
        else:
            print(f"Error fetching data for {crypto} (Status code: {response.status_code})")
    
    if all_data:
        df = pd.concat(all_data, ignore_index=True)
        return df
    else:
        return None


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'


# Transform 
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """
    table_id = 'kornel-crypto-project.crypto_data_project.crypto_df'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        table_id,
        if_exists='replace',  # Specify resolution policy if table name already exists
    )
# export
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """
    table_id = 'kornel-crypto-project.crypto_data_project.data_crypto'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        table_id,
        if_exists='replace',  # Specify resolution policy if table name already exists
    )

