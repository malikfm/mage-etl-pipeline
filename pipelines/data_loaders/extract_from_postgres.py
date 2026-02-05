from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_postgres(*args, **kwargs):
    """
    Template for loading data from a PostgreSQL database.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    execution_date = kwargs['execution_date'].strftime("%Y-%m-%d")
    table_name = kwargs['table_name']
    selected_columns = kwargs['selected_columns']

    query = f"""
    SELECT {selected_columns} FROM {table_name}
    WHERE created_at::DATE = '{execution_date}'
    OR updated_at::DATE = '{execution_date}'
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'source'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        return loader.load(query)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
