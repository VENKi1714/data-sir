

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
#
# if __name__ == '__main__':
#     # Create Spark session
#     spark = SparkSession.builder \
#         .appName("Oracle to Spark") \
#         .config("spark.jars", r"D:\VENKi\ojdbc8.jar") \
#         .config("spark.master", "local[*]") \
#         .getOrCreate()
#
#     # JDBC connection properties
#     jdbcHostname = "LAPTOP-RT7HE0R0"
#     jdbcPort = 1521  # Default port for Oracle
#     jdbcServiceName = "xe"  # SID or Service Name
#     jdbcUrl = f"jdbc:oracle:thin:@{jdbcHostname}:{jdbcPort}/{jdbcServiceName}"
#     jdbcUsername = "sys as sysdba"
#     jdbcPassword = "venki"
#     jdbcDriver = "oracle.jdbc.OracleDriver"
#
#     tab_num = '73.#1'

    # Read data from Oracle
    # table_name = "t_73"
    # t_73_1 = spark.read.format("jdbc") \
    #     .option("url", jdbcUrl) \
    #     .option("query",f"select * from t_73 where table_number ='{tab_num}'") \
    #     .option("user", jdbcUsername) \
    #     .option("password", jdbcPassword) \
    #     .option("driver", jdbcDriver) \
    #     .load()
    #
    # # Show the data
    # t_73_1.show()
    # print(t_73_1.count())
    # oracle_df.printSchema()

################################################################
    # tab_num = '73.#2'

    # Read data from Oracle

    # t_73_2 = spark.read.format("jdbc") \
    #     .option("url", jdbcUrl) \
    #     .option("query", f"select * from t_73 where table_number ='{tab_num}'") \
    #     .option("user", jdbcUsername) \
    #     .option("password", jdbcPassword) \
    #     .option("driver", jdbcDriver) \
    #     .load()
    #
    # # Show the data
    # t_73_2.show()
    # print(t_73_2.count())

######################################################################

    # tab_num = '73.#3'

    # Read data from Oracle
    #
    # t_73_3 = spark.read.format("jdbc") \
    #     .option("url", jdbcUrl) \
    #     .option("query", f"select * from t_73 where table_number ='{tab_num}'") \
    #     .option("user", jdbcUsername) \
    #     .option("password", jdbcPassword) \
    #     .option("driver", jdbcDriver) \
    #     .load()
    #
    # # Show the data
    # t_73_3.show()
    # print(t_73_3.count())

################################################################################################################

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from common_utils import common_config


if __name__ == '__main__':
    cmn_ut = common_config()

#condition 1 => fetch table number =  on table_numberas 1,2,3,
    df1 = cmn_ut.generate_dataframe("73.#1")
    df1.show()
    # print(df1.count())

    df2 = cmn_ut.generate_dataframe("73.#2")
    df2.show()

    df3 = cmn_ut.generate_dataframe("73.#3")
    df3.show()

#(1)########################################################################################################################

# to split the column of amount and replace ','

    df_amount1 = cmn_ut.amount_split(df1)
    df_amount1.show()
    # print(df_amount1.count())


#(1)########################################################################################################################

    # to extract symbol column into interger and aplhabets

    df_split1 = cmn_ut.replace_symbol(df_amount1)
    df_split1.show()
    # print(df_split1.count())

#(2)###########################################################################################################################

    # to split the column of amount and replace ',' for table 2

    df_amount2 = cmn_ut.amount_split(df2)
    df_amount2.show()
    # print(df_amount2.count())

#(2)#############################################################################################################################

    # to extract symbol column into interger and aplhabets for table 2

    df_split2 = cmn_ut.replace_symbol(df_amount2)
    df_split2.show()
    # print(df_split2.count())

#(3)##############################################################################################################################

    # to split the column of amount and replace ',' for table 3

    df_amount3 = cmn_ut.amount_split(df3)
    df_amount3.show()
    # print(df_amount3.count())

#(3)#############################################################################################################################

    # to extract symbol column into interger and aplhabets for table 3

    df_split3 = cmn_ut.replace_symbol(df_amount3)
    df_split3.show()
    # print(df_split3.count())

###############################################################################################################################