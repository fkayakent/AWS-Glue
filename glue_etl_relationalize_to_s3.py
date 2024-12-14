import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
#from awsglue.transforms import Relationalize




glue_source_database = "source database"
glue_source_table = "source table"
glue_temp_storage = "s3://your-temp-bucket/"
glue_relationalize_output_s3_path = "s3://your-bucket/"
dfc_root_table_name = "root" #default value is "roottable"
# End variables to customize with your information
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
glueContext = GlueContext(spark.sparkContext)
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = glue_source_database, table_name = glue_source_table, transformation_ctx = "datasource0")
dfc = Relationalize.apply(frame = datasource0, staging_path = glue_temp_storage, name = dfc_root_table_name, transformation_ctx = "dfc")
blogdata = dfc.select(dfc_root_table_name)
blogdataoutput = glueContext.write_dynamic_frame.from_options(frame = blogdata, connection_type = "s3", connection_options = {"path": glue_relationalize_output_s3_path}, format = "parquet", transformation_ctx = "blogdataoutput")
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()
