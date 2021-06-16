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
    # 结果文件输出
    p_result_table_0 = p_main_dir + "HIS_result/" + "Table_result/Table_result_0"
    p_result_table_1 = p_main_dir + "HIS_result/" + "Table_result/Table_result_1"
    p_result_table_2 = p_main_dir + "HIS_result/" + "Table_result/Table_result_2"
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
    win = Window.partitionBy(["医院ID", "就诊类型", "患者ID", "OUT_ID"])
    df_data_c = df_data_a.withColumn("SEQ", Func.row_number().over( win.orderBy( col("RX_DATE_STD").desc() )  ))\
                            .withColumn("IF_FIRST_RX", when( col("SEQ")==1, 1).otherwise(0) )
                            .withColumn("MAX_SEQ", Funct.max( col("SEQ") ).over( win )  )
    # df_data_c.show()
    # df_data_c_max = df_data_c.groupBy( ["医院ID", "就诊类型", "患者ID", "OUT_ID" ]).agg(Func.max("SEQ").alias("MAX_SEQ") )
    # df_data_c = df_data_c.join( df_data_c_max, on=[ "医院ID", "就诊类型", "患者ID", "OUT_ID" ], how="inner")
    
    
    # 合并上面三个表
    df_data_d = df_data_c.join(df_data_b, on=["医院ID", "就诊类型", "患者ID", "OUT_ID", "RX_DATE_STD"], how="left")
    
    # 是否为换药
    df_data_e = df_data_d.groupBy(["医院ID", "就诊类型", "患者ID", "OUT_ID" ])\
                            .agg( Func.countDistinct("formula").alias("formula_numbers") )\
                            .withColumn("IF_CHANGE_RX",  Func.when(col("formula_numbers")>1, 1).otherwise(0))
    
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
    
    # 计算分组后的sum与count值并加入为新列
    df_patient_std_ps = df_patient_std_pha.groupBy(["年","月","就诊类型","标准医保类型","性别","年龄区间","标准诊断","severe_case","标准科室",
                               "single_or_formula","IF_FIRST_RX","IF_CHANGE_RX","formula","mole_comb",
                               "白细胞计数","c反应蛋白","降钙素原","嗜肺军团菌","肺炎衣原体","肺炎支原体","冠状病毒",
                               "合胞病毒","流感病毒","腺病毒","柯萨奇病毒","鲍曼氏不动杆菌","大肠埃希菌","肺炎克雷伯菌",
                               "肺炎链球菌","金黄色葡萄球菌","流感嗜血菌","嗜麦芽寡养单胞菌","嗜麦芽窄食单胞菌","铜绿假单胞菌",
                               "阴沟肠杆菌","混合感染","心律不齐","其他心血管疾病","脑血管疾病","神经系统疾病","高血糖","高血压",
                               "高血脂","肝功能异常","肾功能异常","结缔组织病","COPD","哮喘","支气管扩张","恶性实体瘤",
                               "HAP患者","seg1_grp1","seg1_grp2","seg2_grp1","seg3_grp1","seg3_grp2","seg3_grp3"]) \
                               .agg( Func.sum( col("金额") ).alias("sales"), Func.countDistinct("患者ID", "就诊序号").alias("patients")  )
    
    rule_ps = ["年","月","就诊类型","标准医保类型","性别","年龄区间","标准诊断","severe_case","标准科室",
                               "single_or_formula","IF_FIRST_RX","IF_CHANGE_RX","formula","mole_comb",
                               "白细胞计数","c反应蛋白","降钙素原","嗜肺军团菌","肺炎衣原体","肺炎支原体","冠状病毒",
                               "合胞病毒","流感病毒","腺病毒","柯萨奇病毒","鲍曼氏不动杆菌","大肠埃希菌","肺炎克雷伯菌",
                               "肺炎链球菌","金黄色葡萄球菌","流感嗜血菌","嗜麦芽寡养单胞菌","嗜麦芽窄食单胞菌","铜绿假单胞菌",
                               "阴沟肠杆菌","混合感染","心律不齐","其他心血管疾病","脑血管疾病","神经系统疾病","高血糖","高血压",
                               "高血脂","肝功能异常","肾功能异常","结缔组织病","COPD","哮喘","支气管扩张","恶性实体瘤",
                               "HAP患者","seg1_grp1","seg1_grp2","seg2_grp1","seg3_grp1","seg3_grp2","seg3_grp3"]
    
    df_patient_std_pha = df_patient_std_pha.join(df_patient_std_ps,rule_ps,"left")
    # %%
    
    # （sales  patients）单独分组计算的结果
    df_table_0 = df_patient_std_pha.select(["年","月","就诊类型","标准医保类型","性别","年龄区间","标准诊断","severe_case","标准科室",
                                   "心律不齐","其他心血管疾病","脑血管疾病","神经系统疾病","高血糖","高血压","高血脂","肝功能异常",
                                   "肾功能异常","结缔组织病","COPD","哮喘","支气管扩张","恶性实体瘤","IF_CHANGE_RX","HAP患者","sales","patients"])
    
    #pfc  sales  patients 
    df_table_1 = df_patient_std_pha.select(["年","月","就诊类型","标准医保类型","性别","年龄区间","标准诊断","severe_case","标准科室",
                                   "MOLECULE","MOLECULE_CATEGORY","BRAND","form","SPEC","PACK_NUMBER","MANUFACTURER",
                                   "白细胞计数","c反应蛋白","降钙素原","嗜肺军团菌","肺炎衣原体","肺炎支原体","冠状病毒",
                                   "合胞病毒","流感病毒","腺病毒","柯萨奇病毒","鲍曼氏不动杆菌","大肠埃希菌","肺炎克雷伯菌",
                                   "肺炎链球菌","金黄色葡萄球菌","流感嗜血菌","嗜麦芽寡养单胞菌","嗜麦芽窄食单胞菌","铜绿假单胞菌",
                                   "阴沟肠杆菌","混合感染","心律不齐","其他心血管疾病","脑血管疾病","神经系统疾病","高血糖","高血压",
                                   "高血脂","肝功能异常","肾功能异常","结缔组织病","COPD","哮喘","支气管扩张","恶性实体瘤","PACK_ID","sales","patients"])
    
    # sales  patients 
    df_table_2 = df_patient_std_pha.select(["年","月","就诊类型","标准医保类型","性别","年龄区间","标准诊断","severe_case","标准科室",
                                       "single_or_formula","IF_FIRST_RX","IF_CHANGE_RX","formula","mole_comb",
                                       "白细胞计数","c反应蛋白","降钙素原","嗜肺军团菌","肺炎衣原体","肺炎支原体","冠状病毒",
                                       "合胞病毒","流感病毒","腺病毒","柯萨奇病毒","鲍曼氏不动杆菌","大肠埃希菌","肺炎克雷伯菌",
                                       "肺炎链球菌","金黄色葡萄球菌","流感嗜血菌","嗜麦芽寡养单胞菌","嗜麦芽窄食单胞菌","铜绿假单胞菌",
                                       "阴沟肠杆菌","混合感染","心律不齐","其他心血管疾病","脑血管疾病","神经系统疾病","高血糖","高血压",
                                       "高血脂","肝功能异常","肾功能异常","结缔组织病","COPD","哮喘","支气管扩张","恶性实体瘤","sales","patients"])
    # %%
    
    # 保存结果
    df_patient_analyse_std = df_patient_analyse_std.repartition(g_partition_num)
    df_patient_analyse_std.write.format("parquet") \
        .mode("overwrite").save(p_patient_union_drug_analyse_out)
    # %%
    df_table_0 = df_table_0.repartition(g_partition_num)
    df_table_0.write.format("parquet") \
        .mode("overwrite").save(p_result_table_0)
    
    # %%
    df_table_1 = df_table_1.repartition(g_partition_num)
    df_table_1.write.format("parquet") \
        .mode("overwrite").save(p_result_table_1)
    
    # %%
    df_table_2 = df_table_2.repartition(g_partition_num)
    df_table_2.write.format("parquet") \
        .mode("overwrite").save(p_result_table_2)
