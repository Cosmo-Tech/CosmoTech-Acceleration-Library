import pyarrow

from cosmotech.coal.store.store import Store

try:
    import pandas as pd


    def store_dataframe(
        table_name: str,
        dataframe: pd.DataFrame,
        replace_existsing_file: bool = False,
        store=Store()
    ):

        data = pyarrow.Table.from_pandas(dataframe)

        store.add_table(table_name=table_name,
                        data=data,
                        replace=replace_existsing_file)


    def convert_store_table_to_dataframe(
        table_name: str,
        store=Store()
    ) -> pd.DataFrame:

        return store.get_table(table_name).to_pandas()

except ModuleNotFoundError:
    pass
