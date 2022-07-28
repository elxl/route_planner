# +
# %reload_ext sparkmagic.magics

import os
from IPython import get_ipython

# set the application name as "<your_gaspar_id>-homework3"
username = os.environ['RENKU_USERNAME']
server = "http://iccluster029.iccluster.epfl.ch:8998"

get_ipython().run_cell_magic(
    'spark',
    line='config', 
    cell="""{{ "name": "{0}-final-proj", "executorMemory": "4G", "executorCores": 4, "numExecutors": 10, "driverMemory": "4G"}}""".format(username)
)
get_ipython().run_line_magic(
    "spark", "add -s {0}-final-proj -l python -u {1} -k".format(username, server)
)

# + language="spark"
# import pyspark.sql.functions as F
# import pyspark
# import pyspark.ml.regression as R
# import pyspark.ml.evaluation as E
# import pyspark.ml.tuning as T
# from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
# from pyspark.ml import Pipeline
# from pyspark.sql.types import *
# -

# ### Select data within 15km from Zürich
#
# The databases from ```istdaten.orc``` of the years 2018 and 2019 are being used until the **12.05.2019** for computing the delays. Also, ```allstops.orc``` containing the stops coordinates help selecting data within a 15km radius from the coordinates (**47.378177**,**8.540192**). The **haversine** distance is used for converting the GPS coordinates into flat coordinates, taking the earth radius into account. The results is stored in a dataframe called ```sbb_zh_15```, which can be stored in HDFS for convenience.

# + language="spark"
#
# save_dataframe = True
#
# # Load the istdaten dataframes
# sbb_2019 = spark.read.orc('/data/sbb/part_orc/istdaten/year=2019')
# sbb_2018 = spark.read.orc('/data/sbb/part_orc/istdaten/year=2018')
#
# # Load the allstops dataframe
# allstops = spark.read.orc('/data/sbb/orc/allstops/000000_0')
#
# # Filter the 2019 dataframe by date to remove datapoints with date > 12.05.2019 
# sbb_2019 = sbb_2019.filter(F.to_timestamp(sbb_2019.betriebstag,'dd.MM.yyyy') < (F.to_timestamp(F.lit('12.05.2019'),'dd.MM.yyyy')))
#
# # Now join both dataframes
# sbb_2018_2019 = sbb_2018.union(sbb_2019)
#
# # Filter by region (Zurich)
# allstops = allstops.withColumnRenamed('stop_id','bpuic') # Rename column to have the same convention as in istdaten
#
# # Add GPS data to istdaten dataframe
# sbb_2018_2019_GPS = sbb_2018_2019.join(allstops , on=['bpuic'] , how = 'left')
#
# # Ball center coordinates
# zh_lat, zh_lon = 47.378177, 8.540192
#
# from pyspark.sql.functions import lit, col, radians, asin, sin, sqrt, cos
#
# # Compute haversine distance for only having 15km around Zurich
# sbb_zh = sbb_2018_2019_GPS.withColumn("dlon", radians(col("stop_lon")) - radians(lit(zh_lon))) \
#                           .withColumn("dlat", radians(col("stop_lat")) - radians(lit(zh_lat))) \
#                           .withColumn("haversine_dist", asin(sqrt(
#                                                                   sin(col("dlat") / 2) ** 2 + cos(radians(lit(zh_lat)))
#                                                                 * cos(radians(col("stop_lat"))) * sin(col("dlon") / 2) ** 2
#                                                                 )
#                                                           ) * 2 * 6372800) \
#                           .drop("dlon", "dlat")
#
# # Filter dataframe based on haversine distance
# sbb_zh_15 = sbb_zh.filter(col('haversine_dist') < 15000).cache()
#
# # Optionally save to HDFS to avoid recomputing each time
# if save_dataframe:
#     sbb_zh_15.write.orc('/group/big-data-projects/sbb_zh_15.orc', mode='overwrite')
#
# print('Nb. of data : {} \nNb. of distinct stops : {}'.format(sbb_zh_15.distinct().count(),
#                                                              sbb_zh_15.select("haltestellen_name").distinct().count()))
# -

# ### Use already pre-processed dataframe to select data within a 15km range
# The dataframe ```stops.orc``` was created using ```allstops.orc``` and only keeping the stops located within our 15km radius. Hence, the ```sbb_zh_15``` can alternatively be created used this one too.

# + language="spark"
# stops = spark.read.orc('/group/Big-data-projects/stop.orc')
# stops = stops.select('stop_name').distinct()
#
# sbb_2019 = spark.read.orc('/data/sbb/part_orc/istdaten/year=2019')
# sbb_2018 = spark.read.orc('/data/sbb/part_orc/istdaten/year=2018')
# sbb_2019 = sbb_2019.withColumnRenamed('haltestellen_name','stop_name')
# sbb_2018 = sbb_2018.withColumnRenamed('haltestellen_name','stop_name')
#
# # Filter by date
# sbb_2019 = sbb_2019.filter(F.to_timestamp(sbb_2019.betriebstag,'dd.MM.yyyy') < (F.to_timestamp(F.lit('12.05.2019'),'dd.MM.yyyy')))
#
# sbb_zh_15 = sbb_2018.union(sbb_2019).cache()
# # Filter by location
# sbb_zh_15 = sbb_zh_15.join(stops, ['stop_name'],'left_semi')
#
#
# # Compare number of stops
# stops_count = stops.count()
# sbb_stops_count = sbb_zh_15.select('stop_name').distinct().count()
#
# print('Nb. of stop names in stops.csv : {} \nNb. of stop names in sbb data : {}'.format(stops_count,sbb_stops_count))
# -

# ### Delays computation
# Here we perform the delays computation using the augmented ```istdaten``` dataframe. In order to do that, we :
#
# * Preprocess the date formats as timestamps.
# * Drop the null values when these values are directly used for delays computation, i.e. when ```an_prognose``` or ```ankunftszeit``` are ```null```.
# * Add the features by which we will later ```groupBy```, i.e. the ```dayOfWeek``` and the ```hourOfDay```
# * Select all interesting features from preprocessed original dataframe ```istdaten```
# * Compute the delays and remove those which are negative (don't take anticipated arrival into account to avoid having over-optimistic confidence intervals)
# * Save dataframe if desired

# Load dataframe if exists

# + language="spark"
# sbb_zh_15 = spark.read.orc('/group/big-data-projects/sbb_zh_15.orc')

# + language="spark"
#
# save_dataframe = True
#
# # Convert dates to timestamps for delay computations
# sbb_zh_15 = sbb_zh_15.withColumn('an_prognose',F.to_timestamp(F.col('an_prognose'),'dd.MM.yyyy HH:mm:ss'))
# sbb_zh_15 = sbb_zh_15.withColumn('ankunftszeit',F.to_timestamp(F.col('ankunftszeit'),'dd.MM.yyyy HH:mm'))
#
# # Drop null values if they are dates, useful for delays computation
# sbb_zh_15_wo_null = sbb_zh_15.where(~(F.col('an_prognose').isNull() | F.col('ankunftszeit').isNull()))
#
# # # Add temporal features to dataframe
# sbb_zh_15_wo_null = (sbb_zh_15_wo_null.withColumn('day',F.dayofweek(F.to_timestamp(F.col('betriebstag'),'dd.MM.yyyy')))
#                                       .withColumn('hour',F.hour(F.to_timestamp(F.col('ankunftszeit'),'yyyy-MM-dd HH:mm:ss'))))
#
# # # Construct updated dataframe
# sbb_zh_delays = sbb_zh_15_wo_null.select(F.date_format(F.to_timestamp(F.col('betriebstag'),'dd.MM.yyyy'),'yyyy-MM-dd').alias('date'),
#                                  'day',
#                                  'hour',
#                                  'stop_name',
#                                  F.col('verkehrsmittel_text').alias('transport_model'),
#                                  F.col('produkt_id').alias('transport_type'),
#                                  F.col('linien_text').alias('transport_line'),
#                                  'ankunftszeit',
#                                  'an_prognose',
#                                  'an_prognose_status')
#
# # # Compute actual delays
# sbb_zh_delays = (sbb_zh_delays.withColumn('arrival_delay',(F.unix_timestamp(F.col('an_prognose'))
#                                                            - F.unix_timestamp(F.col('ankunftszeit')))))
#
# # Remove rows where negative delay (not taken into account)
# sbb_zh_delays = (sbb_zh_delays.withColumn('arrival_delay',F.when(F.col('arrival_delay') < 0, 0)
#                                                       .otherwise(F.col('arrival_delay'))))
#
# # Construct final dataset
# dataset_wNa = sbb_zh_delays.select('date',
#                                    'day',
#                                    'hour',
#                                    'stop_name',
#                                    'transport_model',
#                                    'transport_type',
#                                    'transport_line',
#                                    'arrival_delay')
#
# # Cast the delay to int for convenience
# dataset_wNa = (dataset_wNa.withColumn('arrival_delay',F.col('arrival_delay').cast(IntegerType())))
#
# # Optionally save to HDFS to avoid recomputing each time
# if save_dataframe:
#     dataset_wNa.write.orc('/group/big-data-projects/dataset_wNa.orc', mode='overwrite')
#
# print('Nb. of data : {} \nNb. of distinct stops : {}'.format(dataset_wNa.distinct().count(),
#                                                              dataset_wNa.select("stop_name").distinct().count()))
#
# dataset_wNa.show(n=5)
# -

# ### Grouping and aggregate operations
#
# Here we design the dataframe so that only the important features for distributions fitting are kept. Hence, we ```groupBy``` our actual features (```date```, ```hour``` and ```stop_name```) as well as the ***free features*** (```date```, and ```transport_ID```) which are essentially the features over which we don't want to aggregate and will form the meat of our data.
#
# Load dataframe if desired

# + language="spark"
# dataset_wNa = spark.read.orc('/group/big-data-projects/dataset_wNa.orc')

# + language="spark"
# # Fuse line and type to have less free features
# dataset = dataset_wNa.withColumn('transport_ID',F.concat('transport_type','transport_line')).drop('transport_type','transport_line','transport_model')
#
# # Aggregate for having the mean per all of the features
# dataset_mean = dataset.groupBy('date',
#                                'day',
#                                'hour',
#                                'stop_name').agg(F.mean('arrival_delay').alias('avg_arrival_delay'))
#
# # Save to HDFS in any case
# dataset_mean.write.orc('/group/big-data-projects/dataset_mean_copy.orc', mode='overwrite')
#
# print('Nb. of data : {}'.format(dataset_mean.count()))
#
# dataset_mean.show(n=5)
