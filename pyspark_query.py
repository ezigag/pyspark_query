# Load Weather file from HDFS
lines = sc.textFile("/user/ubuntu/inputs/sudeste.csv")

# Drop Header 
from itertools import islice
lines = lines.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it )
# Split line
weather = lines.map(lambda l: l.split(","))

# Encoding Schema Header
import pyspark
from pyspark.sql.types import *

schemaString = "wsid,wsnm,elvt,lat,lon,inme,city,prov,mdct,date,yr,mo,da,hr,prcp,stp,smax,smin,gbrd,temp,dewp,tmax,dmax,tmin,dmin,hmdy,hmax,hmin,wdsp,wdct,gust"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(",")]

schema = StructType(fields)

# Apply the schema to the RDD.
schemaWeather = sqlContext.createDataFrame(weather, schema)

# Convert type Dataframe
types = ["int", "string", "float", "float", "float", "string", "string", "string", "timestamp", "date",\
		"int","int","int","int","float","float","float","float","float","float",\
		"float","float","float","float","float","float","float","float","float","float","float"]
		
for idx,col_name in enumerate(schemaString.split(",")):
	schemaWeather = schemaWeather.withColumn(col_name, schemaWeather[col_name].cast(types[idx]))

# create table
schemaWeather.registerTempTable("weather")


# Query Part
query = "select B.yr,B.city,B.temp as max_temp" \
		" from	(select A.yr, max(A.tmp) as tmp from (select yr, city, max(temp) as tmp from weather group by yr,city) A group by A.yr) A, weather B"\
		" where A.yr = B.yr and A.tmp = B.temp"\
		" order by A.yr "
		
results = sqlContext.sql(query)
results.show(n=50)



#=============================================================================================================================================================
# "select B.yr,B.city,B.temp"\
		# " from	(select A.yr, max(A.tmp) as tmp from (select yr, city, max(temp) as tmp from weather group by yr,city) A group by A.yr) A, weather B"\
		# " where A.yr = B.yr and A.tmp = B.temp"\
		# " order by A.yr "
		
# select B.yr,B.city,B.temp
# from	(select A.yr, max(A.tmp) as tmp 
         # from (select yr, city, max(temp) as tmp 
               # from weather group by yr,city) A 
               # group by A.yr) A, weather B
# where A.yr = B.yr and A.tmp = B.temp
# order by A.yr 