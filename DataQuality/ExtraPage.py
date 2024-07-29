from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType, StringType, FloatType
import os

spark = SparkSession.builder.appName("FileFormatCheck").getOrCreate()


def readDataDictionaryTable(
        spark: SparkSession,
        jdbc_properties: dict,
        reference_table: str,
        source_url: str,
        df
) -> DataFrame:
    """
    Reads the data dictionary table from MySQL database using the provided JDBC properties and table name.

    :param spark: SparkSession object
    :param jdbc_properties: Dictionary containing JDBC properties such as user and password
    :param reference_table: The name of the table to read from the database
    :param source_url: JDBC URL to connect to the database
    :param df: Placeholder DataFrame argument (not used in this function)
    :return: DataFrame containing the data from the specified table
    """
    data_dict_df = spark.read.jdbc(source_url, reference_table, properties=jdbc_properties)
    return data_dict_df


def validate_csv_with_spark(file_path: str, delimiter: str = ',', require_header: bool = True) -> (DataFrame, str):
    """
    Validates a CSV file by checking its structure and content.

    :param file_path: Path to the CSV file
    :param delimiter: Delimiter used in the CSV file
    :param require_header: Flag indicating whether the CSV file should have a header row
    :return: Tuple containing the DataFrame and validation message
    """
    print(f"Validating CSV file: {file_path}")
    try:
        df = spark.read.option("delimiter", delimiter).option("header", require_header).csv(file_path)

        if not df.columns:
            return None, "File is empty or invalid structure"

        if require_header:
            first_row = df.head(1)
            if not first_row:
                return None, "File does not contain header row"

        print("CSV file is valid")
        return df, "File is valid CSV"

    except Exception as e:
        print(f"Error validating CSV file: {e}")
        return None, f"Error: {e}"


def validate_columns(df: DataFrame, data_dict_df: DataFrame, table_name: str) -> (bool, str):
    """
    Validates the columns of a DataFrame against the data dictionary.

    :param df: DataFrame to be validated
    :param data_dict_df: DataFrame containing data dictionary information
    :param table_name: The name of the table being validated
    :return: Tuple indicating if the columns are valid and a validation message
    """
    print(f"Validating columns for table: {table_name}")
    table_dict_df = data_dict_df.filter(data_dict_df.TableName == table_name)
    data_dict_entries = table_dict_df.collect()

    overall_result = True
    overall_message = ""

    for entry in data_dict_entries:
        attribute_name = entry['AttributeName']
        data_type = entry['DataType']
        is_not_null = entry['IsNotNull']

        print(f"Validating column: {attribute_name}")

        if attribute_name not in df.columns:
            overall_result = False
            overall_message += f"Column {attribute_name} is missing in the CSV file.\n"
            print(overall_message)
            continue

        expected_type = None
        if data_type.lower() == 'integer':
            expected_type = IntegerType()
        elif data_type.lower() == 'string':
            expected_type = StringType()
        elif data_type.lower() == 'float':
            expected_type = FloatType()
        # Add more type mappings as needed

        actual_type = df.schema[attribute_name].dataType

        if expected_type and type(actual_type) != type(expected_type):
            overall_result = False
            overall_message += (f"Column {attribute_name} has incorrect data type. "
                                f"Expected: {expected_type}, Actual: {actual_type}\n")
            print(overall_message)

        if is_not_null.lower() == 'y' and df.filter(df[attribute_name].isNull()).count() > 0:
            overall_result = False
            overall_message += f"Column {attribute_name} contains NULL values but is defined as NOT NULL.\n"
            print(overall_message)

    if overall_result:
        overall_message = "All columns are valid."
    return overall_result, overall_message


def FileFormatCheck(source_file_path: str, source_file_name: str) -> (bool, str):
    """
    Performs a file format check for a given CSV file.

    :param source_file_path: Path to the directory containing the CSV file
    :param source_file_name: Name of the CSV file
    :return: Tuple indicating if the file format is valid and a validation message
    """
    print(f"Starting file format check for {source_file_name} in {source_file_path}")

    # Fetch JDBC properties and table name
    jdbc_properties = {
        "user": "root",
        "password": "Harshal@123"
    }
    source_url = "localhost:3306/your_database"
    reference_table = "data_dictionary_table"

    # Read data dictionary table
    data_dict_df = readDataDictionaryTable(spark, jdbc_properties, reference_table, source_url, None)

    # Combine file path and name to get the full path
    full_file_path = os.path.join(source_file_path, source_file_name)
    print(f"Full file path: {full_file_path}")

    if not os.path.exists(full_file_path):
        message = f"File {full_file_path} does not exist"
        print(message)
        return False, message

    df, message = validate_csv_with_spark(full_file_path)
    if df is None:
        print(message)
        return False, message

    # Extract table name from the file name (if needed)
    table_name = os.path.splitext(source_file_name)[0]

    is_valid, message = validate_columns(df, data_dict_df, table_name)
    print(message)
    return is_valid, message
