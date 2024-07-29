
def ListValueCheck(spark,source_url,jdbc_properties, df, column_name,reference_table, reference_attribute):
    # Load the reference table
    reference_table_df = spark.read \
        .jdbc(source_url, reference_table,  properties=jdbc_properties)

    # Join the target DataFrame with the reference table on the column to check
    invalid_values_df = df.join(reference_table_df, df[column_name] == reference_table_df[reference_attribute],
                                "left_anti")

    # Count the number of invalid records and total rows
    error_record_count = invalid_values_df.count()
    total_record_count = df.count()

    if error_record_count > 0:
        print(f"List Value Check on {column_name} ")
        print(f"Records with invalid values:{error_record_count}")
        invalid_values_df.show()
        return total_record_count, error_record_count
    else:
        print("All records have valid values.")
        return total_record_count, error_record_count