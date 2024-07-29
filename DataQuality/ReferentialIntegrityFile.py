def ReferentialIntegrityCheck(
        spark,
        target_url,
        child_table,
        child_column,
        parent_table,
        parent_column,
        jdbc_properties):
    """
    Perform referential integrity check between a child table and a parent table.

    :param spark: SparkSession object
    :param target_url: JDBC URL for the target database
    :param child_table: Name of the child table
    :param child_column: Name of the foreign key column in the child table
    :param parent_table: Name of the parent table
    :param parent_column: Name of the primary key column in the parent table
    :param jdbc_properties: JDBC properties for connecting to the database
    :return: Tuple containing the result (Pass/Fail), error record count, and total record count
    """
    # Load child and parent tables
    child_df = spark.read.jdbc(url=target_url, table=child_table, properties=jdbc_properties)
    parent_df = spark.read.jdbc(url=target_url, table=parent_table, properties=jdbc_properties)

    # Perform a left anti join to find orphan records in the child table
    orphan_records_df = child_df.join(parent_df, child_df[child_column] == parent_df[parent_column], "left_anti")

    # Count the number of orphan records
    error_record_count = orphan_records_df.count()

    # Count the total number of records in the child table
    total_record_count = child_df.count()

    if error_record_count > 0:
        print(f"Referential Integrity Check between {child_table}.{child_column} and {parent_table}.{parent_column}")
        print(f"Orphan records found: {error_record_count}")
        orphan_records_df.show()
        return  error_record_count, total_record_count
    else:
        print("All records have valid references.")
        return error_record_count, total_record_count
