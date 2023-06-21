import os
import sys
import findspark
import time
from Code.code_max import job1_hospital_mapping, job2_product_mapping, job3_1_data_adding, job3_2_data_adding, job4_panel, \
    job5_max_weight, job6_max_city

if __name__ == "__main__":
    '''初始化spark环境'''
    findspark.init()
    # Path for spark source folder
    os.environ['SPARK_HOME'] = "D:\MAXOS\spark-3.4.0-bin-hadoop3"
    # Append pyspark to Python Path
    sys.path.append("D:\MAXOS\spark-3.4.0-bin-hadoop3\python")

    # 把以前airflow用的参数粘贴过来即可
    kwargs = {"project_name": "TKI",
              "monthly_update": "True",
              "model_month_left": "202101",
              "model_month_right": "202112",
              "all_models": "TKI1",
              "universe_choice": "TKI1:universe_CPA_肿瘤",
              "time_left": "202301",
              "time_right": "202303",
              "first_month": "1",
              "current_month": "3",
              "out_dir": "202303",
              "panel_for_union": "202212/panel_result",
              "if_two_source": "False",
              "add_47": "True",
              "current_year": "2023",
              "source_type": "CPA"}


    time_start = time.time()

    job1_hospital_mapping(kwargs)
    job2_product_mapping(kwargs)
    job3_1_data_adding(kwargs)
    job3_2_data_adding(kwargs)
    job4_panel(kwargs)
    job5_max_weight(kwargs)
    job6_max_city(kwargs)

    time_end = time.time()
    print((time_end - time_start) / 60, 'm')
