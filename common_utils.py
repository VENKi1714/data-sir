from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
# class common_config:
#
#
#     # Create Spark session
#     spark = SparkSession.builder \
#             .appName("Oracle to Spark") \
#             .config("spark.jars", r"D:\VENKi\ojdbc8.jar") \
#             .config("spark.master", "local[*]") \
#             .getOrCreate()
#
#
#     def create_df(self, table_number):
#     # JDBC connection properties
#         jdbcHostname = "LAPTOP-RT7HE0R0"
#         jdbcPort = 1521  # Default port for Oracle
#         jdbcServiceName = "xe"  # SID or Service Name
#         jdbcUrl = f"jdbc:oracle:thin:@{jdbcHostname}:{jdbcPort}/{jdbcServiceName}"
#         jdbcUsername = "sys as sysdba"
#         jdbcPassword = "venki"
#         jdbcDriver = "oracle.jdbc.OracleDriver"
#
#         # Read data from Oracle
#         # table_name = "t_73"
#         oracle_df = self.spark.read.format("jdbc") \
#             .option("url", jdbcUrl) \
#             .option("query", f"select * from t_73 where table_number = {table_number}") \
#             .option("user", jdbcUsername) \
#             .option("password", jdbcPassword) \
#             .option("driver", jdbcDriver) \
#             .load()
#
#         return oracle_df

#############################################################################################################################

from pyspark.sql import SparkSession
from pyspark.sql.functions import *



class common_config:

    spark = SparkSession \
            .builder \
            .config("spark.jars",r"D:\VENKi\ojdbc8.jar")\
            .getOrCreate()

    # to run all the three tables

    def generate_dataframe(self,Table_number):

        username = "sys as sysdba"
        password = "venki"
        driver = "oracle.jdbc.driver.OracleDriver"
        jdbcurl = "jdbc:oracle:thin:@localhost:1521:xe"

        # Reading data from Oracle
        table_DF = self.spark.read \
            .format("jdbc") \
            .option("url", jdbcurl) \
            .option("query", f"select * from t_73 where table_number = '{Table_number}'") \
            .option("user", username) \
            .option("password", password) \
            .option("driver", driver) \
            .load()

        return table_DF

##############################################################################################################################

    # to run all the nine tables
    def generate_dataframe_t_75(self,Table_number):

        username = "sys as sysdba"
        password = "venki"
        driver = "oracle.jdbc.driver.OracleDriver"
        jdbcurl = "jdbc:oracle:thin:@localhost:1521:xe"

        # Reading data from Oracle
        table_DF = self.spark.read \
            .format("jdbc") \
            .option("url", jdbcurl) \
            .option("query", f"select * from t_75 where table_number = '{Table_number}'") \
            .option("user", username) \
            .option("password", password) \
            .option("driver", driver) \
            .load()

        return table_DF



##############################################################################################################################

        # to run all the nine tables
    def generate_dataframe_t_75_1(self, Table_number):
        username = "sys as sysdba"
        password = "venki"
        driver = "oracle.jdbc.driver.OracleDriver"
        jdbcurl = "jdbc:oracle:thin:@localhost:1521:xe"

        # Reading data from Oracle
        table_DF = self.spark.read \
            .format("jdbc") \
            .option("url", jdbcurl) \
            .option("query", f"select * from t_75_1 where table_number = '{Table_number}'") \
            .option("user", username) \
            .option("password", password) \
            .option("driver", driver) \
            .load()

        return table_DF


########################################################################################################################

    # to split the column of amount and replace ','

    def amount_split(self,df):
        # Remove dollar signs and commas from the AMOUNT column
        df = df.withColumn('AMOUNT', regexp_replace('AMOUNT', '[$,]', ''))

        pattern_range = r"(\d+)\s*-\s*(\d+)"
        pattern_more_than = r"(?i)(More\s*than)\s*(\d+)"
        pattern_less_than = r"(?i)(\d+)\s*or\s*Less"

        # less_pattern = r"(?i)(\d+)\s*or\s*Less"
        # range_pattern = r"(\d+)\s*-\s*(\d+)"
        # more_than_pattern = r"(?i)(More\s*than)\s*(\d+)"

        df = df.withColumn("Min_Amount",
                                          when(col("Amount").rlike(pattern_range),
                                               regexp_extract(col("Amount"), pattern_range, 1)) \
                                          .when(col("Amount").rlike(pattern_more_than),
                                                regexp_extract(col("Amount"), pattern_more_than, 2)) \
                                          .when(col("Amount").rlike(pattern_less_than), "0") \
                                          .otherwise(None)).withColumn("Max_Amount",
                                                                       when(col("Amount").rlike(pattern_range),
                                                                            regexp_extract(col("Amount"), pattern_range,
                                                                                           2)) \
                                                                       .when(col("Amount").rlike(pattern_more_than),
                                                                             999999999999) \
                                                                       .when(col("Amount").rlike(pattern_less_than),
                                                                             regexp_extract(col("Amount"),
                                                                                            pattern_less_than, 1)) \
                                                                       .otherwise(None))

        return df


########################################################################################################################

    # to extract symbol column into interger and aplhabets
    def replace_symbol(self,df):
        # Split values into lists
        df = df.withColumn("SYMBOL", explode(split(col("SYMBOL"), ", | and")))

        # Explode the lists into individual rows
        df = df.withColumn("SYMBOL", trim(col("SYMBOL")))

        # Extract integers and alphabets
        df = df.withColumn("symbol_in_number", regexp_extract(col("SYMBOL"), r'(\d+)', 1).cast('int')) \
            .withColumn("symbol_in_alphabet", regexp_extract(col("SYMBOL"), r'([A-Za-z]+)', 1))

        return df

########################################################################################################################

    def add_rename_column(self, df):
        df = df.withColumnRenamed("Terr_code", "Symbol").drop('Deductible').withColumnRenamed("DED_AMOUNT", "DEDUCTIBLE")


        return df

########################################################################################################################

    def select_columns(self,df):
        df =df.select(
        col("STATE_CODE"),
        col("TABLE_NUMBER"),
        col("EFFECTIVE_DATE"),
        col("EXP_DATE"),
        col("MIN_AMOUNT"),
        col("MAX_AMOUNT"),
        col("DEDUCTIBLE"),
        col("SYMBOL_IN_NUMBER"),
        col("SYMBOL_IN_ALPHABET"),
        col("FACTOR")
        )
        return df

########################################################################################################################