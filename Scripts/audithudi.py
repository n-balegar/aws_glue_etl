import sys
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions import col,lit

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

spark = SparkSession.builder.config('spark.serializer','org.apache.spark.serializer.KryoSerializer').config('spark.sql.hive.convertMetastoreParquet','false').getOrCreate()
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


schema = StructType([
        StructField("tableName", StringType(), False),
        StructField("mainifestCount", StringType(), False), 
        StructField("S3FileCount", StringType(), False),             
        StructField("isCountMatched", TimestampType(), False),
		StructField("isProcessCompleted", TimestampType(), False)
])

inputDf = spark.read.option("header",True).csv("s3://aws-glue-murthy/audit.csv",schema=schema)
inputDf.printSchema()
#dd=inputDf.createOrReplaceTempView('temp1')
#spark.sql('select * from temp1').show()


hudiOptions = {
    'className' : 'org.apache.hudi',
    'hoodie.datasource.hive_sync.use_jdbc':'false',
    'hoodie.datasource.write.recordkey.field': 'tableName',
    'hoodie.table.name': 'copy_on_write_glue',
    'hoodie.consistency.check.enabled': 'true',
    'hoodie.datasource.hive_sync.database': 'demo_db',
    'hoodie.datasource.hive_sync.table': 'copy_on_write_glue',
    'hoodie.datasource.hive_sync.enable': 'true',
    'path': 's3://aws-glue-murthy/audit_load'
}

unpartitionDataConfig = {
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor',
    'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator'
}

initLoadConfig = {
    'hoodie.datasource.write.operation': 'insert'
}

combinedConf = {**hudiOptions, **unpartitionDataConfig, **initLoadConfig}

# Write data to S3
glueContext.write_dynamic_frame.from_options(frame = DynamicFrame.fromDF(inputDf, glueContext, "inputDf"), connection_type = "marketplace.spark", connection_options = {"connectionName" : "murthyhudi", **combinedConf})


updDf=inputDf.withColumn("S3FileCount",lit(10)).withColumn("isCountMatched",lit('Y')).withColumn("isProcessCompleted",lit('Y'))
updDf.show(5)
job.commit()
