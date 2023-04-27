import sys
from awsglue.transforms import ApplyMapping
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generado para el nodo Data Catalog table
DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="periodicos",
    table_name="periodico_eltiempo",
    transformation_ctx="DataCatalogtable_node1"
)

# Script generado para el nodo AWS Glue Data Catalog
AWSGlueDataCatalog_node168238 = glueContext.create_dynamic_frame.from_catalog(
    database="periodicos",
    table_name="periodico_elespectador",
    transformation_ctx="AWSGlueDataCatalog_node168238"
)

# Script generado para el nodo ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DataCatalogtable_node1,
    mappings=[
        ("col1", "string", "categoria", "string"),
        ("col2", "string", "link", "string"),
        ("col0", "string", "titulo", "string")
    ],
    transformation_ctx="ApplyMapping_node2"
)

# Script generado para el nodo Change Schema
ChangeSchema_node1682386600797 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node168238,
    mappings=[
        ("col1", "string", "categoria", "string"),
        ("col2", "string", "link", "string"),
        ("col0", "string", "titulo", "string")
    ],
    transformation_ctx="ChangeSchema_node168238"
)

# Script generado para el nodo Data Catalog table
DataCatalogtable_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="periodicos_cloud9",
    table_name="periodicos_p_el_tiempo",
    transformation_ctx="DataCatalogtable_node3"
)

# Script generado para el nodo AWS Glue Data Catalog
AWSGlueDataCatalog_node168238 = glueContext.write_dynamic_frame.from_catalog(
    frame=ChangeSchema_node1682386600797,
    database="periodicos_cloud9",
    table_name="periodicos_p_el_espectador",
    transformation_ctx="AWSGlueDataCatalog_node168238"
)

job.commit()
