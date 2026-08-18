from cosmotech.coal.utils.input_collector import ENVIRONMENT_INPUT_COLLECTOR as Collector

# get parameter toto
toto = Collector.fetch("toto")

# get path to file uploaded as a parameter (parameter.json)
parameter_file_path = Collector.fetch("parameter.json")
# or
parameter_file_path = Collector.fetch("parameter")


# get path to a dataset files (data1.csv)
data1_file_path = Collector.fetch("data1.csv")
# or
data1_file_path = Collector.fetch("data1")
