import os

from mage_ai.io.file import FileIO
from pandas import DataFrame

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_file(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to filesystem.

    Docs: https://docs.mage.ai/design/data-loading#fileio
    """
    execution_date = kwargs['execution_date'].strftime("%Y-%m-%d")
    table_name = kwargs['table_name']
    output_dir = f'data/{table_name}'

    os.makedirs(output_dir, exist_ok=True)

    filepath = f'{output_dir}/{execution_date}.parquet'
    FileIO().export(df, filepath, 'parquet')
