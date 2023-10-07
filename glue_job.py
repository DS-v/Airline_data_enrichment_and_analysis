import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node flights data
flightsdata_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="airlines-table-catalog",
    table_name="airlines_dataset_p3",
    transformation_ctx="flightsdata_node1",
)

# Script generated for node airports data
airportsdata_node1696056568920 = glueContext.create_dynamic_frame.from_catalog(
    database="airlines-table-catalog",
    table_name="dev_airlines_airports_dim",
    redshift_tmp_dir="s3://temp-s3-data/1/",
    transformation_ctx="airportsdata_node1696056568920"
)

# Script generated for node dep>60
dep60_node1696056697175 = Filter.apply(
    frame=flightsdata_node1,
    f=lambda row: (row["depdelay"] > 60),
    transformation_ctx="dep60_node1696056697175",
)

# Script generated for node join for departure
joinfordeparture_node1696056739644 = Join.apply(
    frame1=airportsdata_node1696056568920,
    frame2=dep60_node1696056697175,
    keys1=["airport_id"],
    keys2=["originairportid"],
    transformation_ctx="joinfordeparture_node1696056739644",
)

# Script generated for node Change Schema
ChangeSchema_node1696056909649 = ApplyMapping.apply(
    frame=joinfordeparture_node1696056739644,
    mappings=[
        ("city", "string", "dep_city", "string"),
        ("name", "string", "dep_airport", "string"),
        ("state", "string", "dep_state", "string"),
        ("carrier", "string", "carrier", "string"),
        ("originairportid", "long", "dep_airportid", "long"),
        ("destairportid", "long", "arr_airportid", "long"),
        ("depdelay", "long", "dep_delay", "long"),
        ("arrdelay", "long", "arr_delay", "long"),
    ],
    transformation_ctx="ChangeSchema_node1696056909649",
)

# Script generated for node Join for arrival
Joinforarrival_node1696057067887 = Join.apply(
    frame1=airportsdata_node1696056568920,
    frame2=ChangeSchema_node1696056909649,
    keys1=["airport_id"],
    keys2=["arr_airportid"],
    transformation_ctx="Joinforarrival_node1696057067887",
)

# Script generated for node Change Schema
ChangeSchema_node1696057138439 = ApplyMapping.apply(
    frame=Joinforarrival_node1696057067887,
    mappings=[
        ("city", "string", "arr_city", "VARCHAR"),
        ("name", "string", "arr_airport", "VARCHAR"),
        ("state", "string", "arr_state", "VARCHAR"),
        ("dep_city", "string", "dep_city", "VARCHAR"),
        ("dep_airport", "string", "dep_airport", "VARCHAR"),
        ("dep_state", "string", "dep_state", "VARCHAR"),
        ("carrier", "string", "carrier", "VARCHAR"),
        ("dep_delay", "long", "dep_delay", "BIGINT"),
        ("arr_delay", "long", "arr_delay", "BIGINT"),
    ],
    transformation_ctx="ChangeSchema_node1696057138439",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1696057227593 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1696057138439,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-683934273415-ap-south-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "airlines.daily_flights_fact",
        "connectionName": "airports-data-redshift",
        "preactions": "CREATE TABLE IF NOT EXISTS airlines.daily_flights_fact (arr_city VARCHAR, arr_airport VARCHAR, arr_state VARCHAR, dep_city VARCHAR, dep_airport VARCHAR, dep_state VARCHAR, carrier VARCHAR, depdelay BIGINT, arrdelay BIGINT);",
    },
    transformation_ctx="AmazonRedshift_node1696057227593",
)

job.commit()
