import pandas as pd
df_pandas1 = pd.read_csv('unbalaced_20_80_dataset.csv')
df_pandas2 =  pd.read_csv('final_dataset.csv')
df_pandas = df_pandas1.merge(df_pandas2, how='left')
df_pandas.head()
"""
I got error by using pandas. Error is written below.
  File "parsers.pyx", line 843, in pandas._libs.parsers.TextReader.read_low_memory
  File "parsers.pyx", line 920, in pandas._libs.parsers.TextReader._read_rows
  File "parsers.pyx", line 1065, in pandas._libs.parsers.TextReader._convert_column_data
  File "parsers.pyx", line 1119, in pandas._libs.parsers.TextReader._convert_tokens
  File "parsers.pyx", line 1221, in pandas._libs.parsers.TextReader._convert_with_dtype
  File "parsers.pyx", line 1830, in pandas._libs.parsers._try_int64
numpy.core._exceptions._ArrayMemoryError: Unable to allocate 64.0 KiB for an array with shape (8192,) and data type int64
"""
########################################################################################################################
import dask.dataframe as dd

df_dask1 = dd.read_csv('unbalaced_20_80_dataset.csv')
df_dask2 = dd.read_csv('final_dataset.csv')
df_dask  =  df_dask1.merge(df_dask2, how='left')

"""
Dask library is perfectly read data, also more faster than pandas.
"""
########################################################################################################################
#First, we need to install library which !pip install modin[dask]
import modin.pandas as mpd

df_modin1 = mpd.read_csv('unbalaced_20_80_dataset.csv')
df_modin2 = mpd.read_csv('final_dataset.csv')
df_modin  =  df_modin1.merge(df_modin2, how='left')

"""
I again got memory  error by using pmodin.pandas. 
"""
########################################################################################################################
#First, Ray library and  the necessary packages is installed by using commands
# pip install ray
#pip install modin
#pip install pyarrow

import ray.dataframe as rd

df_ray1 = rd.read_csv('unbalaced_20_80_dataset.csv')
df_ray2 = rd.read_csv('final_dataset.csv')
df_ray  =  df_ray1.merge(df_ray2, how='left')
################################################### TASK STAGE ########################################################
import dask.dataframe as dd
import yaml
import os
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# 1. Read the file with Dask
df_dask1 = dd.read_csv('DataGlacierWeek6/unbalaced_20_80_dataset.csv')
df_dask2 = dd.read_csv('DataGlacierWeek6/final_dataset.csv')
df_dask = df_dask1.merge(df_dask2, how='left')

# 2. Clean column names
def clean_column_names(df):
    df.columns = df.columns.str.replace('[^a-zA-Z0-9]', '_').str.strip()

clean_column_names(df_dask)

# 3. Create a YAML File for Schema
def create_yaml_schema(df, separator):
    schema = {
        'columns': list(df.columns),
        'separator': separator
    }

    with open('schema.yaml', 'w') as yaml_file:
        yaml.dump(schema, yaml_file, default_flow_style=False)

separator = '|'  # Define the separator
create_yaml_schema(df_dask, separator)

# 4. Validate the Schema
def validate_schema(df):
    with open('schema.yaml', 'r') as yaml_file:
        schema = yaml.load(yaml_file, Loader=yaml.FullLoader)

    if sorted(schema['columns']) == sorted(df.columns):
        print("Column names match the schema.")
    else:
        print("Column names do not match the schema.")

validate_schema(df_dask)

# 5. Write the file in pipe-separated text format in gz format
def write_to_pipe_separated_gz(df, output_path, separator='|'):
    df.to_csv(output_path, sep=separator, compression='gzip', index=False)

output_path = 'output_file.txt.gz'
write_to_pipe_separated_gz(df_dask, output_path, separator='|')

# 6. Create a Summary
num_rows = len(df_dask)
num_columns = len(df_dask.columns)
file_size = os.path.getsize(output_path) / (1024 * 1024)  # Size in MB

print(f"Total number of rows: {num_rows}")
print(f"Total number of columns: {num_columns}")
print(f"File size: {file_size} MB")









