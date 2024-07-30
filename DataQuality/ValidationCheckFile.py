import logging
import colorlog
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from datetime import datetime

# Setup colored logging
log_formatter = colorlog.ColoredFormatter(
    '%(log_color)s%(asctime)s - %(levelname)s - %(message)s%(reset)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    reset=True,
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',  # Set INFO logs to green
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    }
)

handler = logging.StreamHandler()
handler.setFormatter(log_formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)


# Assuming you have the NullCheck, ListValueCheck, and ReferentialIntegrityCheck functions defined properly
from DataQuality.NullCheckFile import NullCheck
from DataQuality.NullCheckandUpdateFile import NullCheckAndUpdate
from DataQuality.DuplicateCheckFile import DuplicateCheck
from DataQuality.ListValueCheckFile import ListValueCheck
from DataQuality.ReferentialIntegrityFile import ReferentialIntegrityCheck
from DataQuality.ErrorTableEntryFile import LogErrorEntry
from DataQuality.AuditTableEntryFile import LogAuditEntry
from DataQuality.DQOutputTableEntryFile import WriteDQOutputTable
from DataQuality.DQReadingTableFile import ReadConfigurationTables
from DataQuality.LoggerFile import ConfigureLogging



def PerformValidationCheck(
        spark: SparkSession,
        jdbc_properties: dict,
        mainconfig_table: str,
        dataquality_config_table: str,
        source_database: str,
        target_database: str,
        dataquality_output_table: str
):
    try:
        # Construct URLs
        source_url = f"jdbc:mysql://localhost:3306/{source_database}"
        target_url = f"jdbc:mysql://localhost:3306/{target_database}"

        # Define Output Data List
        output_data = []

        # Read configuration tables using the new function
        logger.info("Reading configuration tables")
        mainconfig_datavalues, DQconfig_datavalues = ReadConfigurationTables(
            spark,
            source_database,
            jdbc_properties,
            mainconfig_table,
            dataquality_config_table
        )
        logger.info("Configuration tables read successfully")

        # Define schema
        output_schema = StructType([
            StructField("ConfigKey", IntegerType(), True),
            StructField("DataQualityKey", IntegerType(), True),
            StructField("ValidationType", StringType(), True),
            StructField("TableName", StringType(), True),
            StructField("Attribute", StringType(), True),
            StructField("TotalRecordCount", IntegerType(), True),
            StructField("ErrorRecordCount", IntegerType(), True),
            StructField("ErrorPercentage", FloatType(), True),
            StructField("ThresholdPercentage", FloatType(), True),
            StructField("Result", StringType(), True)
        ])
        for main_config in mainconfig_datavalues:
            source_system = main_config["SourceSystem"]
            source_file_name = main_config["SourceFileName"]
            source_schema_name = main_config["SourceSchemaName"]

        for dq_config in DQconfig_datavalues:
            config_key = dq_config["ConfigKey"]
            data_quality_key = dq_config["DataQualityKey"]
            validation_type = dq_config["ValidationType"]
            table_name = dq_config["TableName"]
            attribute_name = dq_config["Attribute"]
            reference_table_name = dq_config["ReferenceTableName"]
            threshold_percentage = dq_config["ThresholdPercentage"]
            default_value = dq_config.get("DefaultValue", "default")

            # Read the target table for validation
            logger.info(f"Reading target table: {table_name}")
            target_df = spark.read.jdbc(url=target_url, table=table_name, properties=jdbc_properties)
            logger.info(f"Target table {table_name} read successfully")

            # Example of processing and using these values
            print(f"Processing configuration with ConfigKey: {config_key}")
            print(f"Table: {table_name}, Column Name: {attribute_name}")
            logger.info(f"Processing configuration with ConfigKey: {config_key}")
            logger.info(f"Table: {table_name}, Attribute/Column Name: {attribute_name}")

            # Start time for audit
            start_time = datetime.now()
            logger.info("Starting validation check")

            # Perform validation check based on validation type

            try:
                total_record_count = 0
                error_record_count = 0

                if validation_type == "null_check":
                    error_record_count, total_record_count = NullCheck(target_df, attribute_name)
                elif validation_type == "null_and_update_check":
                    error_record_count, total_record_count = NullCheck(target_df, attribute_name)
                    if error_record_count > 0:
                            NullCheckAndUpdate(spark, jdbc_properties, target_database, table_name, attribute_name, default_value)
                            logger.info(f"Null check performed and table {table_name} updated.")
                    else:
                            logger.info(f"No null values found in column {attribute_name}. Skipping update.")

                if validation_type == 'duplicate_check':
                    error_record_count, total_record_count = DuplicateCheck(target_df, attribute_name)

                elif validation_type == "check_list_values":
                    reference_table = dq_config.get("ReferenceTableName", "")
                    error_record_count, total_record_count = ListValueCheck(
                        spark, source_url, jdbc_properties, target_df, attribute_name,reference_table, "AttributeValue"
                    )

                elif validation_type == "referential_integrity":
                    child_table = table_name
                    child_column = attribute_name.split('.')[1] if '.' in attribute_name else attribute_name
                    parent_table = dq_config.get("ReferenceTableName", "")
                    parent_column = child_column
                    error_record_count, total_record_count = ReferentialIntegrityCheck(
                        spark, target_url, child_table, child_column, parent_table, parent_column, jdbc_properties
                    )

                else:
                    print(f"INVALID VALIDATION TYPE FOUND: {validation_type}")
                    #raise ValueError(f"Unsupported validation type: {validation_type}")

                error_percentage = (error_record_count / total_record_count) if total_record_count > 0 else 0.0

                if error_percentage == threshold_percentage:
                    result = "Pass"
                elif error_percentage < threshold_percentage and error_percentage != 0.0:
                    result = "Partially Pass"
                else:
                    result = "Fail"

                # Ensure all values in result_data are not None
                output_data.append((
                    config_key, data_quality_key, validation_type, table_name, attribute_name, total_record_count,
                    error_record_count, error_percentage, threshold_percentage, result
                ))

                # Write to output table
                logger.info("Writing output to table")
                WriteDQOutputTable(
                    spark, output_data, output_schema, source_url, dataquality_output_table, jdbc_properties
                )
                logger.info("Output written successfully")

            except Exception as e:
                # End time for audit
                end_time = datetime.now()

                # Log error entry
                logger.error(f"Error occurred: {e}")
                LogErrorEntry(
                    spark=spark,
                    jdbc_properties=jdbc_properties,
                    config_key=config_key,
                    audit_key=None,  # You'll need to fetch or generate this ID if required
                    code_name=validation_type,
                    run_id="NA",  # Replace with actual run ID
                    source_system=main_config.get("SourceSystem", ""),
                    schema_name=main_config.get("SourceSchemaName", ""),
                    target_table_name=table_name,
                    source_file_name=main_config.get("SourceFileName", ""),
                    execution_time=datetime.now(),
                    error_details=str(e),
                    source_database=source_database
                )
                raise e  # Re-raise the exception after logging

            # End time for audit
            end_time = datetime.now()
            logger.info(f"Validation check completed. Duration: {end_time - start_time}")

            # Log audit entry
            logger.info("Logging audit entry")
            LogAuditEntry(
                spark=spark,
                jdbc_properties=jdbc_properties,
                config_key=config_key,
                code_name=validation_type,
                run_id="NA",  # Replace with actual run ID
                source_system=main_config.get("SourceSystem", ""),
                schema_name=main_config.get("SourceSchemaName", ""),
                target_table_name=table_name,
                source_file_name=main_config.get("SourceFileName", ""),
                start_time=start_time,
                end_time=end_time,
                inserted_row_count=total_record_count,
                updated_row_count=0,  # Update based on your context
                deleted_row_count=0,  # Update based on your context
                no_change_row_count=0,  # Update based on your context
                status="Completed",
                last_pulled_time=datetime.now(),
                source_database=source_database
            )
            logger.info("Audit entry logged successfully")

        return output_data, output_schema
    finally:
        # Stop SparkSession
        logger.info("Stopping SparkSession")
        spark.stop()
        logger.info("SparkSession stopped")
