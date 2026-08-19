from cosmotech.coal.utils.input_collector import (
    ENVIRONMENT_INPUT_COLLECTOR as Collector,
)

# get parameter toto
toto = Collector.fetch("toto")

# get path to file uploaded as a parameter (parameter_json.json)
parameter_file_path = Collector.fetch("parameter_file.json")
# or
parameter_file_path = Collector.fetch("parameter_file")


# get path to a dataset files (data1.csv)
data1_file_path = Collector.fetch("data1.csv")
# or
data1_file_path = Collector.fetch("data1")

# direct call to sub collector is possible (this is usefull for disambiguation)
data2_file_path = Collector.fetch_workspace("my_data_file")
data3_file_path = Collector.fetch_dataset("my_data_file")
