# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger, LOG_DEBUG_LEVEL


def execute(**kwargs):
    logger = phs3logger(kwargs["job_id"], LOG_DEBUG_LEVEL)
    spark = kwargs['spark']()
    result_path_prefix = kwargs["result_path_prefix"]
    depends_path = kwargs["depends_path"]
    
    ### input args ###
    g_input_paramater = kwargs['g_input_paramater']
    g_partition_num = kwargs['g_partition_num']
    ### input args ###
    
    ### output args ###
    g_out_parameter = kwargs['g_out_parameter']
    ### output args ###

    from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType
    from pyspark.sql.functions import col, date_format, count, isnull, lit
    from pyspark.sql.functions import when, isnan, udf, pandas_udf, PandasUDFType
    from pyspark.sql.window import Window
    from pyspark.sql import functions as Func
    from pyspark.sql import DataFrame, SparkSession    
    
    from typing import Iterator
    
    import pandas as pd
    import re    # %%
    
    ## 参数化文件读入
    # %%
    ## ====== 输入文件和输出文件 ======
    
    # 
    g_whether_save_result = True
    
    p_main_dir = "s3://ph-origin-files/user/zazhao/2020年结果-csv/"
    
    # 检测数据
    p_detection = p_main_dir + "检测"
    
    # 输出文件
    p_detection_std_out = p_main_dir + "HIS_result/" + "clean_detection_result"
    # %%
    
    ## 读取检测数据
    df_raw_detection = spark.read.csv(p_detection, header=True)
    # df_raw_detection.show(1)
    
    df_raw_detection = df_raw_detection.select([ "PATIENT_ID", "VISIT_ID", "ITEM_NAME", "SUBJECT", "REPORT_ITEM_NAME", 
                                                "RESULT", "UNITS", "ABNORMAL_INDICATOR", "REQUESTED_DATE_TIME", 
                                                "RESULTS_RPT_DATE_TIME", "DEPT_NAME"])
    
    df_raw_detection = df_raw_detection.withColumn("VISIT_ID", Func.col("VISIT_ID").cast("int"))\
                                        .withColumn("REQUESTED_DATE_TIME_STD",  date_format("REQUESTED_DATE_TIME", "yyyyMMdd")) \
                                        .withColumn("RESULTS_RPT_DATE_TIME_STD", date_format("RESULTS_RPT_DATE_TIME", "yyyyMMdd"))\
                                        .withColumn("检测信息", Func.concat( col("REPORT_ITEM_NAME"), col("RESULT"), col("UNITS")) )
    # df_raw_detection.where( df_raw_detection["VISIT_ID"].isNull() ).count()
df_raw_detection = df_raw_detection.withColumn("result_type", Func.when(  col("RESULT").cast( "float" ).isNotNull(),
                                                                        "numbers_only" ).otherwise( "character" ))
df_raw_detection = df_raw_detection.withColumn("lab_result",  Func.when(  col("RESULT").cast( "float" ).isNotNull(),
                                                                         col("RESULT").cast( "float" ) ).otherwise( col("RESULT") ))


df_raw_detection = df_raw_detection.withColumn("std_subject", Func.when( col("REPORT_ITEM_NAME").rlike("降钙素|PCT检测"),"降钙素原" )\
                                                                  .when( col("REPORT_ITEM_NAME").rlike("白细胞计数"),"白细胞计数"  )\
                                                                  .when( col("REPORT_ITEM_NAME").rlike("反应蛋白"),"C反应蛋白"  )\
                                                                  .when( col("REPORT_ITEM_NAME").rlike("菌|病毒|支原体|衣原体|结核"),"病原菌检测"  )\
                                                                  .otherwise( "其他检测项目") )


# df_raw_detection =df_raw_detection.filter( col("std_subject") == "病原菌检测")

df_raw_detection = df_raw_detection.withColumn("pathogenic_type", 
                                    Func.when( ( col("std_subject") == "病原菌检测") &( col("REPORT_ITEM_NAME").rlike("嗜肺")  ) , "嗜肺军团菌" )\
                                            .when( ( col("std_subject") == "病原菌检测") &( col("REPORT_ITEM_NAME").rlike("肺炎衣原体")  ) , "肺炎衣原体" )\
                                            .when( ( col("std_subject") == "病原菌检测") &( col("REPORT_ITEM_NAME").rlike("肺炎支原体")  ) , "肺炎支原体" )\
                                            .when( ( col("std_subject") == "病原菌检测") &( col("REPORT_ITEM_NAME").rlike("冠状病毒")  ) , "冠状病毒" )\
                                            .when( ( col("std_subject") == "病原菌检测") &( col("REPORT_ITEM_NAME").rlike("合胞病毒")  ) , "合胞病毒" )\
                                            .when( ( col("std_subject") == "病原菌检测") &( col("REPORT_ITEM_NAME").rlike("流感病毒")  ) , "流感病毒" )\
                                            .when( ( col("std_subject") == "病原菌检测") &( col("REPORT_ITEM_NAME").rlike("腺病毒")  ) , "腺病毒" )\
                                            .when( ( col("std_subject") == "病原菌检测") &( col("REPORT_ITEM_NAME").rlike("柯萨奇病毒")  ) , "柯萨奇病毒" )\
                                            .when( ( col("std_subject") == "病原菌检测") &( col("REPORT_ITEM_NAME").rlike("鲍曼氏不动")  ) , "鲍曼氏不动" )\
                                            .when( ( col("std_subject") == "病原菌检测") &( col("REPORT_ITEM_NAME").rlike("大肠埃希菌")  ) , "大肠埃希菌" )\
                                            .when( ( col("std_subject") == "病原菌检测") &( col("REPORT_ITEM_NAME").rlike("克雷伯")  ) , "肺炎克雷伯菌" )\
                                            .when( ( col("std_subject") == "病原菌检测") &( col("REPORT_ITEM_NAME").rlike("肺炎链球菌")  ) , "肺炎链球菌" )\
                                            .when( ( col("std_subject") == "病原菌检测") &( col("REPORT_ITEM_NAME").rlike("金黄色葡萄球菌")  ) , "金黄色葡萄球菌" )\
                                            .when( ( col("std_subject") == "病原菌检测") &( col("REPORT_ITEM_NAME").rlike("流感嗜血菌")  ) , "流感嗜血菌" )\
                                            .when( ( col("std_subject") == "病原菌检测") &( col("REPORT_ITEM_NAME").rlike("嗜麦芽寡养")  ) , "嗜麦芽寡养单胞菌" )\
                                            .when( ( col("std_subject") == "病原菌检测") &( col("REPORT_ITEM_NAME").rlike("嗜麦芽窄食")  ) , "嗜麦芽窄食单胞菌" )\
                                            .when( ( col("std_subject") == "病原菌检测") &( col("REPORT_ITEM_NAME").rlike("铜绿")  ) , "铜绿假单胞菌" )\
                                            .when( ( col("std_subject") == "病原菌检测") &( col("REPORT_ITEM_NAME").rlike("阴沟肠杆菌")  ) , "阴沟肠杆菌" )\
                                            .otherwise("未知") )
## 还有其他的细胞病毒
## 巨细胞病毒

# value_range 是什么？
# 此个项目里 value_range 没有
df_raw_detection = df_raw_detection.withColumn("std_result", Func.when( ( col("std_subject") == "降钙素原") &( col("result_type") == "numbers_only" )&\
                                                                            ( col("lab_result")<0.1), "<0.1(无感染)" )\
                                                                   .when( ( col("std_subject") == "降钙素原") &( col("result_type") == "numbers_only" )&\
                                                                            ( col("lab_result")>=0.1)& ( col("lab_result")<0.25 ), "0.1-0.25(可能感染，不建议抗生素)" )\
                                                                   .when( ( col("std_subject") == "降钙素原") &( col("result_type") == "numbers_only" )&\
                                                                            ( col("lab_result")>=0.25)& ( col("lab_result")<=0.5 ), "0.25-0.5(局部感染，建议抗生素)" )\
                                                                    .when( ( col("std_subject") == "降钙素原") &( col("result_type") == "numbers_only" )&\
                                                                            ( col("lab_result")>0.5), ">0.5(严重感染及脓毒症)" )\
                                                                    .when( ( col("std_subject") == "降钙素原") &( col("result_type") == "character"), ">0.5(严重感染及脓毒症)" )\
 
                                                                   .when( ( col("std_subject") == "白细胞计数") &( col("result_type") == "numbers_only")&\
                                                                             ( col("lab_result")>9.3 ), "H" )\
                                                                   .when( ( col("std_subject") == "白细胞计数") &( col("result_type") == "numbers_only")&\
                                                                             ( col("lab_result")<=9.3 ), "L" )\
                                                                   .when( ( col("std_subject") == "白细胞计数") &( col("result_type") == "character"), "L" )\
                                               
                                                                   .when( ( col("std_subject") == "C反应蛋白") &( col("result_type") == "numbers_only")&\
                                                                             ( col("UNITS").rlike("d") )&( col("lab_result")>=0.8), "H" )\
                                                                   .when( ( col("std_subject") == "C反应蛋白") &( col("result_type") == "numbers_only")&\
                                                                             ( col("UNITS").rlike("d") )&( col("lab_result")<0.8), "L" )\
                                                                   .when( ( col("std_subject") == "C反应蛋白") &( col("result_type") == "numbers_only")&\
                                                                             ( ~col("UNITS").rlike("d") )&( col("lab_result")>=0.8), "H" )\
                                                                   .when( ( col("std_subject") == "C反应蛋白") &( col("result_type") == "numbers_only")&\
                                                                             ( ~col("UNITS").rlike("d") )&( col("lab_result")>=0.8), "L" )\
                                                                   .when( ( col("std_subject") == "C反应蛋白") &( col("result_type") == "character")&\
                                                                            ( col("UNITS").rlike("mg/dl") ), "L" )\
                                                                   .when( ( col("std_subject") == "C反应蛋白") &( col("result_type") == "character")&\
                                                                            ( ~col("UNITS").rlike("d") )&( col("lab_result").rlike("\\<|\\＜") ), "L" )\
                                                                   .when( ( col("std_subject") == "C反应蛋白") &( col("result_type") == "character")&\
                                                                            ( ~col("UNITS").rlike("d") ), "H" )\
                                                                   .when( ( col("std_subject") == "病原菌检测") &( col("lab_result").rlike("可疑|弱阳") ), "弱阳性" )\
                                                                   .when( ( col("std_subject") == "病原菌检测") &( col("lab_result").rlike("阳|\\+|未|无") ), "阳性" )\
                                                                   .when( ( col("std_subject") == "病原菌检测") &( col("lab_result").rlike("未|无|阴|\\-") ), "阴性" )\
                                                                   .when( ( col("std_subject") == "病原菌检测") &( col("pathogenic_type") == "肺炎支原体") &\
                                                                          ( col("result_type") == "numbers_only") & (col("lab_result")<0.025), "阴性" )\
                                                                   .when( ( col("std_subject") == "病原菌检测") &( col("pathogenic_type") == "肺炎支原体") &\
                                                                          ( col("result_type") == "numbers_only")& (col("lab_result")>=0.025), "阳性" )\
                                                                   .when( ( col("std_subject") == "病原菌检测") &( col("pathogenic_type") == "肺炎支原体")& \
                                                                         ( col("result_type") == "character"), "阴性" )\
                                                                   .otherwise("未知")
                                              )
# .when( ( col("std_subject") == "病原菌检测") &( col("pathogenic_type") == "肺炎支原体") &\
#      ( col("value_range").rlike("\\<1\\:40") ) & ( col("result_type") == "numbers_only")&\
#             (col("lab_result")<0.025), "阴性" )\
# .when( ( col("std_subject") == "病原菌检测") &( col("pathogenic_type") == "肺炎支原体") &\
#      ( col("value_range").rlike("\\<1\\:40") ) & ( col("result_type") == "numbers_only")&\
#             (col("lab_result")>=0.025), "阳性" )\

df_std_detection = df_raw_detection.filter( ( col("std_result").isNotNull() )& ( col("pathogenic_type").isNotNull())|( col("pathogenic_type") == "肺炎支原体") )

# df_raw_detection.show(30)                                               
    # %%
    
    ## 保存数据
    df_std_detection.repartition(g_partition_num)\
        .write.mode("overwrite").parquet( p_detection_std_out)
