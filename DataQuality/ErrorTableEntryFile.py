from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from datetime import datetime

def LogErrorEntry(
        spark: SparkSession,
        jdbc_properties: dict,
        config_key: int,
        audit_key: int,
        code_name: str,
        run_id: str,
        source_system: str,
        schema_name: str,
        target_table_name: str,
        source_file_name: str,
        execution_time: datetime,
        error_details: str,
        source_database: str

):
    """
    Log an error entry into the error table.

    :param spark: SparkSession object
    :param jdbc_properties: JDBC properties for connecting to the database
    :param config_key: Configuration key
    :param audit_key: Audit key
    :param code_name: Code name
    :param run_id: Run ID
    :param source_system: Source system
    :param schema_name: Schema name
    :param target_table_name: Target table name
    :param source_file_name: Source file name
    :param execution_time: Execution time
    :param error_details: Error details
    """
    # Define schema
    schema = StructType([
        StructField("ConfigKey", IntegerType(), True),
        StructField("AuditKey", IntegerType(), True),
        StructField("CodeName", StringType(), True),
        StructField("RunId", StringType(), True),
        StructField("SourceSystem", StringType(), True),
        StructField("SchemaName", StringType(), True),
        StructField("TargetTableName", StringType(), True),
        StructField("SourceFileName", StringType(), True),
        StructField("ExecutionTime", StringType(), True),
        StructField("error_details", StringType(), True)
    ])

    # Create DataFrame
    result_data = [(config_key, audit_key, code_name, run_id, source_system, schema_name, target_table_name,
                    source_file_name, execution_time.strftime('%Y-%m-%d %H:%M:%S'), error_details)]

    result_df = spark.createDataFrame(result_data, schema=schema)

    # Write the result to errortable
    result_df.write.jdbc(url=f"jdbc:mysql://localhost:3306/{source_database}", table="errortable", mode="append", properties=jdbc_properties)
