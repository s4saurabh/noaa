NOAA_INPUT_BUCKET="s3://noaa-global-hourly-pds/20*/"

OUTPUT_BUCKET="<<S3 bucket>>"
output_prefix = f"s3://{OUTPUT_BUCKET}/<<prefix>>/"

import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import concat

spark = SparkSession.builder.appName("NoaaETL").getOrCreate()

df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(NOAA_INPUT_BUCKET)

# Select specific columns and exclude missing values
subset_df = df \
.select("DATE", "NAME", "WND") \
.withColumnRenamed("NAME", "location_name") \
.filter(F.split(df.WND, ",")[3] != '9999')

# Parse year and windspeed - scaled backed from the raw data
wind_date_df = subset_df \
.withColumn("wind_speed", F.split(subset_df.WND, ",")[3].cast(DoubleType())/10 ) \
.withColumn("measurement_year", F.year(subset_df.DATE))\
.select("location_name", "measurement_year", "wind_speed")

# Find yearly min, avg and max wind speed for each location 
agg_wind_df = wind_date_df \
.groupBy("location_name","measurement_year") \
.agg(F.min(wind_date_df.wind_speed).alias("min_wind_speed"),\
F.avg(wind_date_df.wind_speed).alias("avg_wind_speed"),\
F.max(wind_date_df.wind_speed).alias("max_wind_speed")\
)

# Writing the file output to your local S3 bucket
current_time = datetime.now().strftime('%Y-%m-%d-%H-%M')
agg_wind_df.write.csv(f"{output_prefix}/{current_time}/")

print("Finished writing NOAA data out to: ", output_prefix)
