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
    import re    
    # %%
    
    ##
    
    ##
    # %%
    p_main_dir = "s3://ph-origin-files/user/zazhao/2020年结果-csv/"
    
    # 输入数据目录
    p_patient_std_with_more_target = p_main_dir + "HIS_result/" + "patient_with_more_target_result"
    
    # 输出数据目录
    p_patient_with_select = p_main_dir + "HIS_result/" + "patient_with_select_result"
    # %%
    
    ## 读取添加各种标签后的病人处方数据
    df_patient_std_with_more_target = spark.read.parquet(p_patient_std_with_more_target)
    # %%
    
    ## 筛选研究范围
    df_patient_std_with_more_target =  df_patient_std_with_more_target.withColumn("uni_code",  Func.concat(col("医院ID"), col("患者ID") ) )
    
    
    # 使用替加环素的患者 
    ## 及只要使用过 替加环素的患者,就找出来
    df_condition_uni_code = df_patient_std_with_more_target.filter(col("molecule") == "替加环素").select("uni_code").distinct()
                
    # 筛选患者
    # 筛选目标分子  
    # 筛选目标剂型
    df_patient_object = df_patient_std_with_more_target.filter(  col("标准诊断").rlike("社区获得性肺炎|肺部感染|呼吸道感染|支气管肺炎|肺炎"))\
                            .filter(col("MOLECULE_CATEGORY").isNotNull())\
                            .filter(~col("剂型").rlike("滴耳剂|眼用凝胶|滴眼剂|凝胶剂|软膏剂") )                           
    
    ## 过滤不含替加环素的患者 的数据
    df_patient_object = df_patient_object.join(df_condition_uni_code, on="uni_code", how="left_anti" )
    
    ## 是否需要筛选住院部分
    
    
    ## 是否需要 分类类别 重新定义
    # delivery_base = delivery_base.withColumn("mole_category",   Func.when(col("mole_category") == "环素类","四环素类") \
    #                                                                  .otherwise(col("mole_category")) )
    
    
    # print( df_patient_std_with_more_target.count(), df_patient_object.count() )
    # %%
    
    df_patient_object = df_patient_object.repartition( g_partition_num)
    df_patient_object.write.format("parquet") \
        .mode("overwrite").save(p_patient_with_select)
