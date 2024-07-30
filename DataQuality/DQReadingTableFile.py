from pyspark.sql import SparkSession
from DataQuality.LoggerFile import ConfigureLogging
# Configure logging
#tableLogger = ConfigureLogging()
def ReadConfigurationTables(spark: SparkSession, source_database: str, jdbc_properties: dict,
                              mainconfig_table: str, dataquality_config_table: str):

    try:
        # Read configuration tables
        #tableLogger.info("Reading configuration tables")
        mainconfig_df = spark.read.jdbc(url=f"jdbc:mysql://localhost:3306/{source_database}",
                                        table=mainconfig_table, properties=jdbc_properties)
        dataquality_config_df = spark.read.jdbc(url=f"jdbc:mysql://localhost:3306/{source_database}",
                                                table=dataquality_config_table, properties=jdbc_properties)
        #tableLogger.info("Configuration tables read successfully")

        # Collect and process values from mainconfig_table
        mainconfig_datavalues = []
        #tableLogger.info("Processing mainconfig_table data")
        for row in mainconfig_df.collect():
            mainconfig_datavalues.append({
                "ConfigKey": row["ConfigKey"],
                "ExecutionLayer": row["ExecutionLayer"],
                "SourceSystem": row["SourceSystem"],
                "SourceType": row["SourceType"],
                "SourceFilePath": row["SourceFilePath"],
                "SourceFileName": row["SourceFileName"],
                "SourceFileDelimiter": row["SourceFileDelimiter"],
                "SourceSchemaName": row["SourceSchemaName"],
                "SourceTableName": row["SourceTableName"],
                "TargetType": row["TargetType"],
                "TargetFilePath": row["TargetFilePath"],
                "TargetFileDelimiter": row["TargetFileDelimiter"],
                "TargetSchemaName": row["TargetSchemaName"],
                "TemporaryTargetTable": row["TemporaryTargetTable"],
                "TargetTableName": row["TargetTableName"],
                "LoadType": row["LoadType"],
                "PrimaryKey": row["PrimaryKey"],
                "EffectiveDateColumn": row["EffectiveDateColumn"],
                "MD5Columns": row["MD5Columns"],
                "ActiveFlag": row["ActiveFlag"],
                "FullDataRefreshFlag": row["FullDataRefreshFlag"]
            })
        #tableLogger.info("Mainconfig_table data processed")

        # Collect and process values from dataquality_config_table
        DQconfig_datavalues = []
        #tableLogger.info("Processing dataquality_config_table data")
        for row in dataquality_config_df.collect():
            DQconfig_datavalues.append({
                "ConfigKey": row["ConfigKey"],
                "DataQualityKey": row["DataQualityKey"],
                "ValidationType": row["ValidationType"],
                "TableName": row["TableName"],
                "Attribute": row["AttributeName"],
                "ReferenceTableName": row["ReferenceTableName"],
                "ThresholdPercentage": row["ThresholdPercentage"]
            })
        #tableLogger.info("Dataquality_config_table data processed")

        return mainconfig_datavalues, DQconfig_datavalues

    except Exception as e:
        #tableLogger.error(f"Error occurred while reading configuration tables: {e}")
        raise e

# this is my code