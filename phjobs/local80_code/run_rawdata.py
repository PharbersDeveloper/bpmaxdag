import os
import sys
import findspark
import time
from Code.code_rawdata import pretreat, needclean, check

if __name__ == "__main__":
    '''初始化spark环境'''
    findspark.init()
    # Path for spark source folder
    os.environ['SPARK_HOME'] = "D:\MAXOS\spark-3.4.0-bin-hadoop3"
    # Append pyspark to Python Path
    sys.path.append("D:\MAXOS\spark-3.4.0-bin-hadoop3\python")

    # 把以前airflow用的参数粘贴过来即可
    kwargs = {"project_name":"TKI",
                "outdir":"202303",
                "path_change_file":"D:/Auto_MAX/MAX/TKI/TKI问题医院替换.csv",
                "cut_time_left":"202301",
                "cut_time_right":"202303",
                "current_year":"2023",
                "current_month":"3",
                "if_union":"False"}

    time_start = time.time()

    pretreat(kwargs)
    needclean(kwargs)
    #check(kwargs)

    time_end = time.time()
    print((time_end - time_start) / 60, 'm')
