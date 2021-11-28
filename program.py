from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F
import json
sc = SparkContext('local')
spark = SparkSession(sc)

df = spark.read.option("multiline","true").json('clean_bookings.json')

## Get a data from configuration file
transforms=[]
birthdate_to_age_array = []
hot_encoding = []
fill_empty_values = []
for data in df.collect():
    # Source
    dataset = data.source.dataset
    format_ = data.source.format
    path = data.source.path
    # Target
    path_output = data.sink.path
    dataset_output = data.sink.dataset
    format_output = data.sink.format
    for i in data.transforms:
        transforms.append(i.transform)
        if i.transform == 'birthdate_to_age':
            for t in i.fields:
                birthdate_to_age_array.append(json.loads(t))
        if i.transform == 'hot_encoding':
            hot_encoding = i.fields
        if i.transform == 'fill_empty_values':
            for t in i.fields:
                fill_empty_values.append(json.loads(t))
print('Get a data from configuration file')
## load dataset from source
path_dataset = path + dataset + '.' + format_
df_read = spark.read.format(format_).option("header", "true").load(path_dataset)
print('load dataset from source')
# birthdate_to_age: Given a date, it computes the age of a person and creates a new column with the result.
for i in birthdate_to_age_array:
    df_read = df_read.withColumn(i['new_field'], F.rint(F.months_between(F.current_date(),F.col(i['field'])) / F.lit(12)))
print('birthdate_to_age: Given a date, it computes the age of a person and creates a new column with the result.')

# hot_encoding: Given a categorical column with n possible values, it replaces it with n binary columns. For example, given the column “color”
# with 3 possible values: “blue”, “red” and “green”, we will create 3 columns: “is_blue”, “is_red” and “is_green”, that will be 1 or 0 depending of the
# value of the original column.
for j in hot_encoding:    
    columns = []
    for i in df_read.collect():
        columns.append(i[j])
    columns_1 = list(set(columns))
    for i in columns_1:
        name_of_column = 'is_' + i
        df_read = df_read.withColumn(name_of_column, F.when(F.col('vehicle_category') == i, 1).otherwise(0))
print('hot_encoding.')

# fill_empty_values: It replaces the empty values of a column with another value. The value to be replaced with is passed as a parameter and it can
# be a constant or one of the following keywords: “mean”, “median” or “mode”.
# If we pass one of these keywords, we replace the empty values with the mean / median / mode of the rest of the elements of the column.
for i in fill_empty_values:
    df_read = df_read.withColumn(i['field'], F.when(F.col(i['field']).isNull(), i['value']).otherwise(F.col(i['field'])))

# Save
file_path = path_output + dataset_output + '.' + format_output
#df_read.write.format(format_output).save(file_path)
df_read.write.mode('overwrite').parquet(file_path)
print('Finished...')