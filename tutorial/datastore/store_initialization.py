# Fresh store each time
store = Store(reset=True)

# Persistent store at default location
store = Store()

# Persistent store at custom location
import pathlib

custom_path = pathlib.Path("/path/to/custom/location")
store = Store(store_location=custom_path)
