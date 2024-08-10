from pyspark.sql.functions import *
from pyspark.sql.window import Window
from common_utils import common_config


if __name__ == '__main__':
    cmn_ut = common_config()

#condition 1 => fetch table number =  on table_numbers 10,11,12,13,14,15,16,17,18,19,20


    dt10 = cmn_ut.generate_dataframe_t_75_1("75.#10")
    dt10 = cmn_ut.add_rename_column(dt10)
    dt10 = cmn_ut.amount_split(dt10)
    dt10 = cmn_ut.replace_symbol(dt10)
    dt10 = cmn_ut.select_columns(dt10)
    dt10.show()
    print(dt10.count())

    dt11 = cmn_ut.generate_dataframe_t_75_1("75.#11")
    dt11 = cmn_ut.add_rename_column(dt11)
    dt11 = cmn_ut.amount_split(dt11)
    dt11 = cmn_ut.replace_symbol(dt11)
    dt11 = cmn_ut.select_columns(dt11)
    dt11.show()
    print(dt11.count())

    dt12 = cmn_ut.generate_dataframe_t_75_1("75.#12")
    dt12 = cmn_ut.add_rename_column(dt12)
    dt12 = cmn_ut.amount_split(dt12)
    dt12 = cmn_ut.replace_symbol(dt12)
    dt12 = cmn_ut.select_columns(dt12)
    dt12.show()
    print(dt12.count())

    dt13 = cmn_ut.generate_dataframe_t_75_1("75.#13")
    dt13 = cmn_ut.add_rename_column(dt13)
    dt13 = cmn_ut.amount_split(dt13)
    dt13 = cmn_ut.replace_symbol(dt13)
    dt13 = cmn_ut.select_columns(dt13)
    dt13.show()
    print(dt13.count())

    dt14 = cmn_ut.generate_dataframe_t_75_1("75.#14")
    dt14 = cmn_ut.add_rename_column(dt14)
    dt14 = cmn_ut.amount_split(dt14)
    dt14 = cmn_ut.replace_symbol(dt14)
    dt14 = cmn_ut.select_columns(dt14)
    dt14.show()
    print(dt14.count())

    dt15 = cmn_ut.generate_dataframe_t_75_1("75.#15")
    dt15 = cmn_ut.add_rename_column(dt15)
    dt15 = cmn_ut.amount_split(dt15)
    dt15 = cmn_ut.replace_symbol(dt15)
    dt15 = cmn_ut.select_columns(dt15)
    dt15.show()
    print(dt15.count())

    dt16 = cmn_ut.generate_dataframe_t_75_1("75.#16")
    dt16 = cmn_ut.add_rename_column(dt16)
    dt16 = cmn_ut.amount_split(dt16)
    dt16 = cmn_ut.replace_symbol(dt16)
    dt16 = cmn_ut.select_columns(dt16)
    dt16.show()
    print(dt16.count())

    dt17 = cmn_ut.generate_dataframe_t_75_1("75.#17")
    dt17 = cmn_ut.add_rename_column(dt17)
    dt17 = cmn_ut.amount_split(dt17)
    dt17 = cmn_ut.replace_symbol(dt17)
    dt17 = cmn_ut.select_columns(dt17)
    dt17.show()
    print(dt17.count())

    dt18 = cmn_ut.generate_dataframe_t_75_1("75.#18")
    dt18 = cmn_ut.add_rename_column(dt18)
    dt18 = cmn_ut.amount_split(dt18)
    dt18 = cmn_ut.replace_symbol(dt18)
    dt18 = cmn_ut.select_columns(dt18)
    dt18.show()
    print(dt18.count())

    dt19 = cmn_ut.generate_dataframe_t_75_1("75.#19")
    dt19 = cmn_ut.add_rename_column(dt19)
    dt19 = cmn_ut.amount_split(dt19)
    dt19 = cmn_ut.replace_symbol(dt19)
    dt19 = cmn_ut.select_columns(dt19)
    dt19.show()
    print(dt19.count())

    dt20 = cmn_ut.generate_dataframe_t_75_1("75.#20")
    dt20 = cmn_ut.add_rename_column(dt20)
    dt20 = cmn_ut.amount_split(dt20)
    dt20 = cmn_ut.replace_symbol(dt20)
    dt20 = cmn_ut.select_columns(dt20)
    dt20.show()
    print(dt20.count())