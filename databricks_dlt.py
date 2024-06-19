import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Set the AWS S3 path
s3_path = "s3://example-eu-central-1/STUDENT.parquet"

# Optionally, set AWS credentials if not using IAM roles
spark.conf.set("fs.s3a.access.key", dbutils.secrets.get(scope="aws", key="aws-access-key"))
spark.conf.set("fs.s3a.secret.key", dbutils.secrets.get(scope="aws", key="aws-secret-key"))

@dlt.table(
    name="student",
    comment="Raw data from S3",
    table_properties={"quality": "bronze"}
)
def read_from_s3():
    df = (spark.read.format("parquet")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(s3_path))
    return df
