
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from datetime import datetime

def LogAuditEntry(
        spark: SparkSession,
        jdbc_properties: dict,
        config_key: int,
        code_name: str,
        run_id: str,
        source_system: str,
        schema_name: str,
        target_table_name: str,
        source_file_name: str,
        start_time: datetime,
        end_time: datetime,
        inserted_row_count: int,
        updated_row_count: int,
        deleted_row_count: int,
        no_change_row_count: int,
        status: str,
        last_pulled_time: datetime,
        source_database: str

):
    """
    Log an entry into the audit table.

    :param spark: SparkSession object
    :param jdbc_properties: JDBC properties for connecting to the database
    :param config_key: Configuration key
    :param code_name: Code name
    :param run_id: Run ID
    :param source_system: Source system
    :param schema_name: Schema name
    :param target_table_name: Target table name
    :param source_file_name: Source file name
    :param start_time: Start time
    :param end_time: End time
    :param inserted_row_count: Inserted row count
    :param updated_row_count: Updated row count
    :param deleted_row_count: Deleted row count
    :param no_change_row_count: No change row count
    :param status: Status of the operation
    :param last_pulled_time: Last pulled time
    """
    # Define schema
    schema = StructType([
        StructField("ConfigKey", IntegerType(), True),
        StructField("CodeName", StringType(), True),
        StructField("RunId", StringType(), True),
        StructField("SourceSystem", StringType(), True),
        StructField("SchemaName", StringType(), True),
        StructField("TargetTableName", StringType(), True),
        StructField("SourceFileName", StringType(), True),
        StructField("StartTime", StringType(), True),
        StructField("EndTime", StringType(), True),
        StructField("InsertedRowCount", IntegerType(), True),
        StructField("UpdatedRowCount", IntegerType(), True),
        StructField("DeletedRowCount", IntegerType(), True),
        StructField("NoChangeRowCount", IntegerType(), True),
        StructField("Status", StringType(), True),
        StructField("LastPulledTime", StringType(), True)
    ])

    # Create DataFrame
    result_data = [(config_key, code_name, run_id, source_system, schema_name, target_table_name, source_file_name,
                    start_time.strftime('%Y-%m-%d %H:%M:%S'), end_time.strftime('%Y-%m-%d %H:%M:%S'),
                    inserted_row_count, updated_row_count, deleted_row_count, no_change_row_count,
                    status, last_pulled_time.strftime('%Y-%m-%d %H:%M:%S'))]

    result_df = spark.createDataFrame(result_data, schema=schema)

    # Write the result to audittable
    result_df.write.jdbc(url=f"jdbc:mysql://localhost:3306/{source_database}", table="audittable", mode="append", properties=jdbc_properties)
