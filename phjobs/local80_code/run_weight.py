import os
import sys
import findspark
import time
from Code.code_weight import default, get_weight_gr, gradient_descent

if __name__ == "__main__":
    '''初始化spark环境'''
    findspark.init()
    # Path for spark source folder
    os.environ['SPARK_HOME'] = "D:\MAXOS\spark-3.4.0-bin-hadoop3"
    # Append pyspark to Python Path
    sys.path.append("D:\MAXOS\spark-3.4.0-bin-hadoop3\python")

    # 把以前airflow用的参数粘贴过来即可
    kwargs = {"project_name":"Gilead",
                "outdir":"202101",
                "market_city_brand":"乙肝:北京市_3|上海市_3",
                "universe_choice":"乙肝:universe_传染,乙肝_2:universe_传染,乙肝_3:universe_传染",
                "year_list":"2018,2019",
                "job_choice":"weight",
                "ims_sales_path":"D:/Auto_MAX/MAX/Common_files/extract_data_files/cn_IMS_Sales_Fdata_202301.csv"}
    '''
    kwargs = {"project_name": "Gilead",
     "universe_choice": "乙肝:universe_传染_2020,乙肝_2:universe_传染_2020,乙肝_3:universe_传染_2020",
     "all_models": "乙肝,乙肝_2,乙肝_3",
     "job_choice": "weight_default"}
    '''

    time_start = time.time()

    # default(kwargs)
    get_weight_gr(kwargs)
    gradient_descent(kwargs)

    time_end = time.time()
    print((time_end - time_start) / 60, 'm')
