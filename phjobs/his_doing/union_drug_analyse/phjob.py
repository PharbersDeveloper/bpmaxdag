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
    
    g_whether_save_result = True
    
    p_main_dir = "s3://ph-origin-files/user/zazhao/2020年结果-csv/"
    
    
    # 输入目录
    p_patient_with_select_out =  p_main_dir + "HIS_result/patient_with_select_result"
    
    # 输出文件
    p_patient_union_drug_analyse_out = p_main_dir + "HIS_result/" + "patient_union_drug_analyse_result"

    # %%
    
    ## 读取处方表
    df_patient_object = spark.read.parquet(p_patient_with_select_out)

    # %%
    
    # 单药 和 联药分析
    
    
    # 联用种类个数
    df_data_temp = df_patient_object.select(["医院ID", "就诊类型", "患者ID", "OUT_ID", "标准处方日期", "MOLECULE_CATEGORY", "MOLECULE"])\
                                    .withColumn("标准处方日期", col("标准处方日期").cast("int"))
    
    df_data_a = df_patient_object.withColumn("RX_DATE_STD", col("标准处方日期")) \
                                .groupBy(["医院ID", "就诊类型", "患者ID", "OUT_ID", "RX_DATE_STD"])\
                                .agg( Func.countDistinct("MOLECULE_CATEGORY").alias("分子种类数") )
    
    # df_data_a.orderBy(["医院ID", "就诊类型", "患者ID", "OUT_ID", "RX_DATE_STD"]).show(20)
    
    # 联用方式
    df_data_b = df_patient_object.withColumn("RX_DATE_STD", col("标准处方日期")) \
                                .groupBy(["医院ID", "就诊类型", "患者ID", "OUT_ID", "RX_DATE_STD"])\
                                .agg(  Func.collect_set(col("MOLECULE_CATEGORY")).alias("formula"), \
                                     Func.collect_set( col("MOLECULE")  ).alias("mole_comb") )
    df_data_b = df_data_b.withColumn("formula", Func.concat_ws("+", col("formula")) )\
                            .withColumn("mole_comb", Func.concat_ws("+", col("mole_comb")))
    
    
    
    # 是否为初始药
    win = Window.partitionBy(["医院ID", "就诊类型", "患者ID", "OUT_ID"]).orderBy( col("RX_DATE_STD").desc() )
    df_data_c = df_data_a.withColumn("SEQ", Func.row_number().over( win ))\
                            .withColumn("IF_FIRST_RX", when( col("SEQ")==1, 1).otherwise(0) )
    # df_data_c.show()
    df_data_c_max = df_data_c.groupBy( ["医院ID", "就诊类型", "患者ID", "OUT_ID" ]).agg(Func.max("SEQ").alias("MAX_SEQ") )
    # df_data_c_max.show()
    
    df_data_c = df_data_c.join( df_data_c_max, on=[ "医院ID", "就诊类型", "患者ID", "OUT_ID" ], how="inner")
    # df_data_c_max.show()
    
    
    # 合并上面三个表
    df_data_d = df_data_c.join(df_data_b, on=["医院ID", "就诊类型", "患者ID", "OUT_ID", "RX_DATE_STD"], how="left")
    
    # 是否为换药
    df_data_e = df_data_d.groupBy(["医院ID", "就诊类型", "患者ID", "OUT_ID" ])\
                            .agg( Func.countDistinct("formula").alias("formula_numbers") )\
                            .withColumn("IF_CHANGE",  Func.when(col("formula_numbers")>1, 1).otherwise(0))
    
    # 合并
    df_data_f = df_data_d.join( df_data_e, on=["医院ID", "就诊类型", "患者ID", "OUT_ID" ], how="left")
    df_data_f = df_data_f.withColumnRenamed("RX_DATE_STD", "标准处方日期" )
    
    # 和处方数据进行匹配
    df_patient_analyse_std = df_patient_object.join(df_data_f, on=["医院ID", "就诊类型", "患者ID", "OUT_ID", "标准处方日期"], how="left" )\
                                            .withColumn("single_or_formula", Func.when( col("formula").rlike("\+")
                                                                ,"联用").otherwise("单药") )
    df_patient_analyse_std = df_patient_analyse_std.withColumn("single_or_formula", Func.when( col("formula").isin(
                                                                    ["头孢菌素类+头孢菌素类","青霉素类+青霉素类","其他抗生素+其他抗生素",
                                                                       "头孢菌素酶抑制剂+头孢菌素酶抑制剂","四环素类+四环素类",
                                                                       "氨基糖甙+氨基糖甙","氟喹诺酮+氟喹诺酮"]
                                                                ),"单药").otherwise( col("single_or_formula") ) )

    # %%
    
    # 保存结果
    df_patient_analyse_std = df_patient_analyse_std.repartition(2)
    df_patient_analyse_std.write.format("parquet") \
        .mode("overwrite").save(p_patient_union_drug_analyse_out)

