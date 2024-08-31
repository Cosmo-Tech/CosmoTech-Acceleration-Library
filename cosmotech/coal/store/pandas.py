import pyarrow

from cosmotech.coal.store.store import Store

try:
    import pandas as pd


    def store_dataframe(table_name: str, dataframe: pd.DataFrame, replace_existsing_file: bool = False):

        data = pyarrow.Table.from_pandas(dataframe)

        _s = Store()

        _s.add_table(table_name=table_name,
                     data=data,
                     replace=replace_existsing_file)


    def convert_store_table_to_dataframe(table_name: str) -> pd.DataFrame:
        _s = Store()
        return _s.get_table(table_name).to_pandas()

except ModuleNotFoundError:
    pass
