from pyspark.sql.functions import *
from pyspark.sql.window import Window
from common_utils import common_config


if __name__ == '__main__':
    cmn_ut = common_config()

#condition 1 => fetch table number =  on table_numberas 1,2,3,4,5,6,7,8,9


    dt1 = cmn_ut.generate_dataframe_t_75("75.#1")
    dt1 = cmn_ut.add_rename_column(dt1)
    dt1 = cmn_ut.amount_split(dt1)
    dt1 = cmn_ut.replace_symbol(dt1)
    dt1 = cmn_ut.select_columns(dt1)
    dt1.show()
    print(dt1.count())

    dt2 = cmn_ut.generate_dataframe_t_75("75.#2")
    dt2 = cmn_ut.add_rename_column(dt2)
    dt2 = cmn_ut.amount_split(dt2)
    dt2 = cmn_ut.replace_symbol(dt2)
    dt2 = cmn_ut.select_columns(dt2)
    dt2.show()
    print(dt2.count())


    dt3 = cmn_ut.generate_dataframe_t_75("75.#3")
    dt3 = cmn_ut.add_rename_column(dt3)
    dt3 = cmn_ut.amount_split(dt3)
    dt3 = cmn_ut.replace_symbol(dt3)
    dt3 = cmn_ut.select_columns(dt3)
    dt3.show()
    print(dt3.count())


    dt4 = cmn_ut.generate_dataframe_t_75("75.#4")
    dt4 = cmn_ut.add_rename_column(dt4)
    dt4 = cmn_ut.amount_split(dt4)
    dt4 = cmn_ut.replace_symbol(dt4)
    dt4 = cmn_ut.select_columns(dt4)
    dt4.show()
    print(dt4.count())



    dt5 = cmn_ut.generate_dataframe_t_75("75.#5")
    dt5 = cmn_ut.add_rename_column(dt5)
    dt5 = cmn_ut.amount_split(dt5)
    dt5 = cmn_ut.replace_symbol(dt5)
    dt5 = cmn_ut.select_columns(dt5)
    dt5.show()
    print(dt5.count())



    dt6 = cmn_ut.generate_dataframe_t_75("75.#6")
    dt6 = cmn_ut.add_rename_column(dt6)
    dt6 = cmn_ut.amount_split(dt6)
    dt6 = cmn_ut.replace_symbol(dt6)
    dt6 = cmn_ut.select_columns(dt6)
    dt6.show()
    print(dt6.count())

    dt7 = cmn_ut.generate_dataframe_t_75("75.#7")
    dt7 = cmn_ut.add_rename_column(dt7)
    dt7 = cmn_ut.amount_split(dt7)
    dt7 = cmn_ut.replace_symbol(dt7)
    dt7 = cmn_ut.select_columns(dt7)
    dt7.show()
    print(dt7.count())

    dt8 = cmn_ut.generate_dataframe_t_75("75.#8")
    dt8 = cmn_ut.add_rename_column(dt8)
    dt8 = cmn_ut.amount_split(dt8)
    dt8 = cmn_ut.replace_symbol(dt8)
    dt8 = cmn_ut.select_columns(dt8)
    dt8.show()
    print(dt8.count())

    dt9 = cmn_ut.generate_dataframe_t_75("75.#9")
    dt9 = cmn_ut.add_rename_column(dt9)
    dt9 = cmn_ut.amount_split(dt9)
    dt9 = cmn_ut.replace_symbol(dt9)
    dt9 = cmn_ut.select_columns(dt9)
    dt9.show()
    print(dt9.count())


########################################################################################################################



########################################################################################################################

