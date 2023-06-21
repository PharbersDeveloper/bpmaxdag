import os
import sys
import findspark
import time
from Code.code_factor import job1, job2, job3

if __name__ == "__main__":
    '''初始化spark环境'''
    findspark.init()
    # Path for spark source folder
    os.environ['SPARK_HOME'] = "D:\MAXOS\spark-3.4.0-bin-hadoop3"
    # Append pyspark to Python Path
    sys.path.append("D:\MAXOS\spark-3.4.0-bin-hadoop3\python")

    # 把以前airflow用的参数粘贴过来即可
    kwargs = {"project_name":"神州",
            "outdir":"201912",
            "model_month_right":"201912",
            "model_month_left":"201901",
            "all_models":"SZ1",
            "universe_choice":"SZ1:universe_onc",
            "max_file":"MAX_result_201801_201912_city_level",
            "ims_version":"202010",
            "ims_info_auto":"False"}

    time_start = time.time()

    job1(kwargs)
    job2(kwargs)
    job3(kwargs)

    time_end = time.time()
    print((time_end - time_start) / 60, 'm')
