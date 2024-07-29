'''
def WriteDQOutputTable(spark, output_data, output_schema, source_url, dataquality_output_table,
                                      jdbc_properties):
    # Create DataFrame
    result_df = spark.createDataFrame(output_data, schema=output_schema)

    # Write the result to dataquality_outputtable
    result_df.write.jdbc(url=source_url, table=dataquality_output_table, mode="append", properties=jdbc_properties)


'''

from DataQuality.LoggerFile import ConfigureLogging
# Configure logging at the start
#tableLogger = ConfigureLogging()

def WriteDQOutputTable(spark, output_data, output_schema, source_url, dataquality_output_table, jdbc_properties):
    try:
        #tableLogger.info("Creating DataFrame for output data.")
        # Create DataFrame
        result_df = spark.createDataFrame(output_data, schema=output_schema)

        #tableLogger.info(f"Writing the result to {dataquality_output_table}.")
        # Write the result to dataquality_outputtable
        result_df.write.jdbc(url=source_url, table=dataquality_output_table, mode="append", properties=jdbc_properties)

        #tableLogger.info(f"Successfully wrote data to {dataquality_output_table}.")
    except Exception as e:
        #tableLogger.error(f"Error occurred while writing to {dataquality_output_table}: {e}")
        raise