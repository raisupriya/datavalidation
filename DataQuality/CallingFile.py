'''
import os
os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\HP\\AppData\\Local\\Microsoft\\WindowsApps\\python.exe'
'''

import logging
import colorlog
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from datetime import datetime


# Assuming you have the NullCheck, ListValueCheck, and ReferentialIntegrityCheck functions defined properly
from DataQuality.NullCheckFile import NullCheck
from DataQuality.NullCheckandUpdateFile import NullCheckAndUpdate
from DataQuality.ListValueCheckFile import ListValueCheck
from DataQuality.ReferentialIntegrityFile import ReferentialIntegrityCheck
from DataQuality.ErrorTableEntryFile import LogErrorEntry
from DataQuality.AuditTableEntryFile import LogAuditEntry
from DataQuality.DQOutputTableEntryFile import WriteDQOutputTable
from DataQuality.DQReadingTableFile import ReadConfigurationTables
from DataQuality.ValidationCheckFile import PerformValidationCheck


# Create SparkSession
spark = SparkSession.builder \
    .appName("Perform Validation Check") \
    .getOrCreate()

# Example user inputs
jdbc_properties = {
    "user": "root",
    "password": "8901pa$$word",
    "driver": "com.mysql.cj.jdbc.Driver"
}
mainconfig_table = "config_table"
dataquality_config_table = "data_quality_config"
target_database = "university_database"
source_database = "data_quality"
dataquality_output_table = "quality_output"

# Call the PerformValidationCheck function
PerformValidationCheck(
    spark,
    jdbc_properties,
    mainconfig_table,
    dataquality_config_table,
    source_database,
    target_database,
    dataquality_output_table
)
