def DuplicateCheck(df, column):
    # Calculate total record count
    total_record_count = df.count()

    # Find duplicate rows
    duplicates_df = df.groupBy(column).count().filter("count > 1").select(column)

    # Join with the original DataFrame to get the duplicate rows
    duplicate_rows_df = df.join(duplicates_df, on=column, how='inner')

    # Calculate error record count (number of duplicate rows)
    error_record_count = duplicate_rows_df.count()

    # Print information
    print(f"Duplicate count in column {column}: {error_record_count}")
    print("Duplicate rows:")
    duplicate_rows_df.show()

    # Return total record count and error record count
    return total_record_count, error_record_count


