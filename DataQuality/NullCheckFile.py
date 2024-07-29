
from pyspark.sql.functions import col

def NullCheck(df, column):
    nulls_in_column = df.filter(df[column].isNull())
    null_count = nulls_in_column.count()
    error_record_count = null_count
    total_record_count = df.count()
    print(f"Null Value Check on {column}")
    print(f"Nulls in column {column}: {null_count}")
    nulls_in_column.show()
    return total_record_count,error_record_count