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
    
    ## 参数化文件读入

    # %%
    ## ====== 输入文件和输出文件 ======
    
    # 
    g_whether_save_result = True
    
    p_main_dir = "s3://ph-origin-files/user/zazhao/2020年结果-csv/"
    
    # 检测数据
    p_detection = p_main_dir + "检测"
    
    # 需要关联的数据
    p_mapping_file = p_main_dir + "清洗规则/"
    p_patient_target = p_mapping_file + "标签病人层面_测试用.csv"
    p_out_id_mapping = p_mapping_file + "门诊诊疗周期.csv"
    p_molecule_mapping = p_mapping_file + "20个分子分类.csv"
    # 输入目录
    p_patient_std_dir = "s3://ph-origin-files/user/zazhao/2020年结果-csv/HIS_result/clean_patient_data_result"
    
    # 输出文件
    p_patient_std_correlation_out = p_main_dir + "HIS_result/" + "correlation_data_result"

    # %%
    
    ## 读取清洗后的处方数据
    # p_patient_std_dir = "s3://ph-origin-files/user/zazhao/2020年结果-csv/HIS_result/clean_data_result/"
    df_patient_std = spark.read.parquet(p_patient_std_dir)

    # %%
    
    # 1. 给病人打标签
    
    ## 原始为feather格式文件,转换为CSV格式
    df_target_patient_mapping = spark.read.csv(p_patient_target, header=True) \
                            .withColumnRenamed("PATIENT_ID", "患者ID")\
                            .withColumnRenamed("VISIT_ID", "就诊序号")
    
    ## 本身病人标签是没有 OUT_ID 的
    # 需要从诊疗周期表匹配得到
    df_target_patient_mapping = df_target_patient_mapping.drop("OUT_ID")
    
    df_target_patient_mapping = df_target_patient_mapping.withColumn("标准处方日期", date_format("处方日期", "yyyyMMdd") )\
                                                            .withColumn("标准入院时间", date_format("入院时间", "yyyyMMdd") )\
                                                            .withColumn("标准出院时间", date_format("出院时间", "yyyyMMdd") ) \
                                                            .drop("处方日期", "入院时间", "出院时间")
    # df_target_patient_mapping.printSchema()
    df_patient_std = df_patient_std.join( df_target_patient_mapping, on=["医院ID","患者ID", "就诊序号", 
                                                                     "标准处方日期", "标准入院时间", "标准出院时间" ], how="left")
    
    logger.debug( "无法被打标签的病人样本:  ", df_patient_std.join( df_target_patient_mapping, on=["医院ID","患者ID", "就诊序号" ], how="anti").count() )
    # %%
    
    # 2. 分子类别匹配
    df_molecule_class = spark.read.csv(p_molecule_mapping, header=True)\
                            .select(['分子名', 'Molecule', 'mole_category'])\
                            .withColumnRenamed("Molecule", "MOLECULE_OTHER")\
                            .withColumnRenamed("分子名", "MOLECULE")\
                            .withColumnRenamed("mole_category", "MOLECULE_CATEGORY")
    
    logger.debug( "无法被匹配到标准分子类别的:  ", df_patient_std.join(df_molecule_class, on="MOLECULE",how="anti" ).count() )
    df_patient_std =  df_patient_std.join(df_molecule_class, on="MOLECULE",how="left" )
    # df_patient_with_mol_class.show(1, vertical=True)

    # %%
    
    ## 4. 从门诊诊疗周期表中匹配得到 OUT_ID 
    df_out_id_mapping = spark.read.csv(p_out_id_mapping, header=True)
    
    # OUT_ID匹配  （对门诊部分14天为1诊疗周期的内容进行生成，代码未找到，但找到了结果文件，可直接进行匹配）
    # 门诊部分信息
    df_out_id_mapping = df_out_id_mapping.select(["HCODE", "PATIENT_ID", "VISIT_ID", "OUT_ID"]).distinct()
    df_out_id_mapping = df_out_id_mapping.select([ col("HCODE").alias("医院ID"),
                                                  col("PATIENT_ID").alias("患者ID"),
                                                  col("VISIT_ID").alias("就诊序号"),
                                                  col("OUT_ID")])
    
    df_patient_std = df_patient_std.join(df_out_id_mapping,on=["医院ID", "患者ID", "就诊序号"], how="left") 
    # 如果OUT_ID 为空就用 就诊序号代替            
    df_patient_std = df_patient_std.withColumn( "OUT_ID", when( col("OUT_ID").isNull() ,df_patient_std["就诊序号"]) \
                                                        .otherwise( col("OUT_ID") ) )
    

    # %%
    
    ## 保存关联后的结果
    df_patient_std = df_patient_std.repartition( g_partition_num)
    df_patient_std.write.format("parquet") \
        .mode("overwrite").save(p_patient_std_correlation_out)

