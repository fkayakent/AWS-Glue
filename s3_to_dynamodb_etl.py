import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
DataSource0 = glueContext.create_dynamic_frame.from_options(connection_type ="s3",connection_options={"paths": ["s3://output/"], "recurse": True},format="parquet",transformation_ctx = "DataSource0")
glueContext.write_dynamic_frame_from_options(frame= DataSource0, connection_type= 'dynamodb', connection_options={'dynamodb.output.tableName': 'reddit', 'dynamodb.throughput.write.percent': '1.0'})
job.commit()
