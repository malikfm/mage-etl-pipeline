import os

if 'condition' not in globals():
    from mage_ai.data_preparation.decorators import condition


@condition
def evaluate_condition(*args, **kwargs) -> bool:
    execution_date = kwargs['execution_date'].strftime("%Y-%m-%d")
    table_name = kwargs['table_name']
    filepath = f'data/{table_name}/{execution_date}.parquet'
    if os.path.exists(filepath):
        return False  # Don't execute block
    return True  # Execute block
