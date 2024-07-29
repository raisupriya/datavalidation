from py4j.java_gateway import java_import,logger
from DataQuality.NullCheckFile import NullCheck


def NullCheckAndUpdate(spark, jdbc_properties, target_database, table_name, attribute_name, default_value):
    try:
        # Construct the JDBC URL
        jdbc_url = f"jdbc:mysql://localhost:3306/{target_database}"

        # Update query to set attribute_name to default_value where it is null
        update_query = f"""
            UPDATE {table_name}
            SET {attribute_name} = COALESCE({attribute_name}, '{default_value}')
            WHERE {attribute_name} IS NULL
        """

        # Import Java's DriverManager
        java_import(spark._jvm, "java.sql.DriverManager")

        # Establish JDBC connection
        conn = spark._jvm.DriverManager.getConnection(
            jdbc_url, jdbc_properties["user"], jdbc_properties["password"]
        )

        # Create statement and execute update query
        stmt = conn.createStatement()
        stmt.executeUpdate(update_query)

        #logger.info(f"Table {table_name} updated successfully.")
        print(f"Table {table_name} updated successfully.")

        # Read the updated table into a DataFrame
        updated_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=jdbc_properties)

        # Calculate total_record_count and error_record_count
        total_record_count = updated_df.count()
        error_record_count = NullCheck(updated_df, attribute_name)



    except Exception as e:
        #logger.error(f"Error updating table {table_name}: {str(e)}")
        raise e

    finally:
        # Close resources
        if 'conn' in locals():
            conn.close()
        if 'stmt' in locals():
            stmt.close()
