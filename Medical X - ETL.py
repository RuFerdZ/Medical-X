import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame


# Script generated for node Medical Data transform
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    selected_df = dfc.select(list(dfc.keys())[0]).toDF()
    selected_df = selected_df.withColumn("age", selected_df["age"].cast("int"))
    selected_df.createOrReplaceTempView("medicalInsuranceData")
    totals = spark.sql(
        "select age, CASE WHEN sex = 'female' THEN 0 ELSE 1 END as sex, bmi, children, CASE WHEN smoker = 'yes' THEN 1 ELSE 0 END as smoker, region, charges FROM medicalInsuranceData"
    )
    results = DynamicFrame.fromDF(totals, glueContext, "results")
    return DynamicFrameCollection({"results": results}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Medical Insurance Data
MedicalInsuranceData_node1701016120535 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://medical-x/raw-datasets/Health Insuarance Data.csv"],
        "recurse": True,
    },
    transformation_ctx="MedicalInsuranceData_node1701016120535",
)

# Script generated for node Medical Data schema configurations
MedicalDataschemaconfigurations_node1701029129038 = ApplyMapping.apply(
    frame=MedicalInsuranceData_node1701016120535,
    mappings=[
        ("age", "string", "age", "double"),
        ("sex", "string", "sex", "string"),
        ("bmi", "string", "bmi", "double"),
        ("children", "string", "children", "int"),
        ("smoker", "string", "smoker", "string"),
        ("region", "string", "region", "string"),
        ("charges", "string", "charges", "double"),
    ],
    transformation_ctx="MedicalDataschemaconfigurations_node1701029129038",
)

# Script generated for node Medical Data transform
MedicalDatatransform_node1701029263818 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {
            "MedicalDataschemaconfigurations_node1701029129038": MedicalDataschemaconfigurations_node1701029129038
        },
        glueContext,
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1701030086734 = SelectFromCollection.apply(
    dfc=MedicalDatatransform_node1701029263818,
    key=list(MedicalDatatransform_node1701029263818.keys())[0],
    transformation_ctx="SelectFromCollection_node1701030086734",
)

# Script generated for node Amazon S3
AmazonS3_node1701030380570 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFromCollection_node1701030086734,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://medical-x/transformed-data/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1701030380570",
)

job.commit()
