from cosmotech.coal.store.store import Store

try:
    import pyarrow as pa


    def store_table(
        table_name: str,
        data: pa.Table,
        replace_existsing_file: bool = False,
        store=Store()
    ):

        store.add_table(table_name=table_name,
                        data=data,
                        replace=replace_existsing_file)


    def convert_store_table_to_dataframe(table_name: str, store=Store()) -> pa.Table:
        return store.get_table(table_name)

except ModuleNotFoundError:
    pass
