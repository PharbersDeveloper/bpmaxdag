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
    from pyspark.sql.functions import col, date_format, count, isnull, lit,first
    from pyspark.sql.functions import when, isnan, pandas_udf, PandasUDFType, datediff
    from pyspark.sql import functions as Func
    from pyspark.sql import DataFrame
    from pyspark.sql import Window
    from typing import Iterator
    import pandas as pd
    import re
    # %%
    
    ##
    ##
    # %%
    ## ====== 输入文件和输出文件 ======
    
    # 
    g_whether_save_result = True
    
    p_main_dir = "s3://ph-origin-files/user/zazhao/2020年结果-csv/"
    
    
    # 输入目录
    p_patient_union_drug_analyse_std = "s3://ph-origin-files/user/zazhao/2020年结果-csv/HIS_result/patient_union_drug_analyse_result/"
    p_detection_std = "s3://ph-origin-files/user/zazhao/2020年结果-csv/HIS_result/clean_detection_result/"
    
    # 输出文件
    p_patient_std_compare_out = p_main_dir + "HIS_result/" + "compare_data_result_with_detection"
    p_quinolone_result_dir = p_patient_std_compare_out + "/quinolone_result"
    p_cephalosporin_result_dir =p_patient_std_compare_out + "/cephalosporin_result"
    # %%
    
    ## 读取病人处方数据
    df_patient_union_analyse_std = spark.read.parquet(p_patient_union_drug_analyse_std)

    # %%
    
    ## 读取检测数据
    df_detection_std = spark.read.parquet(p_detection_std)
    # 选择所有 病原菌检测  的类别
    df_detection_std = df_detection_std.filter( col("std_subject")=="病原菌检测")
    

    # %%
    
    
    df_temp_detection = df_detection_std.select(["患者ID", "就诊序号", "检测日期" ] )\
                                            .withColumn("处方检测匹配日期",  date_format( col("检测日期"), "yyyyMMdd" ) )\
                                            .withColumn("检测日期",  date_format( col("检测日期"), "yyyyMMdd" ) )
                                            
    
    # ## 因为每个病人一天会有多个检测,所以需要去重
    df_temp_detection = df_temp_detection.distinct()
    # 就诊序号有空值存在
    df_temp_detection = df_temp_detection.where( col("就诊序号").isNotNull() )
    
    
    df_temp_patient = df_patient_union_analyse_std.withColumn("处方检测匹配日期", date_format( col("处方日期"), "yyyyMMdd" ) )\
                                                    .withColumn("标准处方日期_INT",  col("标准处方日期").cast("int"))
    
    logger.debug( df_temp_detection.count()  )
    logger.debug( df_temp_patient.join(df_temp_detection,  on= ["患者ID", "就诊序号", "处方检测匹配日期"], how="inner").count()  )
    
    # 此处只考虑存在一次检测的情况下
    # 区分检测前和 检测后
    df_patient_with_detection = df_temp_patient.join(df_temp_detection,  on= ["患者ID", "就诊序号", "处方检测匹配日期"], how="inner") \
                                                .select( ["医院ID","患者ID","OUT_ID","就诊类型", "检测日期"]).distinct()
    
    
    
    #### 判断是否存在多次检测
    # df_patient_with_detection.select(["医院ID","患者ID","就诊序号", "OUT_ID","就诊类型", "标准入院时间", "标准出院时间", "标准处方日期", "检测日期"])\
    #                             .dropDuplicates( ["医院ID","患者ID","OUT_ID","就诊类型", "检测日期"])\
    #                             .withColumn( "多次检测",  Func.count("*").over( Window.partitionBy( "医院ID","患者ID","OUT_ID","就诊类型" ) ) )\
    #                             .filter( col("多次检测")>1)\
    #                             .show(10, False )
    ###
    
    
    # 在 "医院ID","患者ID","OUT_ID","就诊类型" 层面下 出现多次检测日期
    # 选择最小的检测日期
    df_patient_with_detection = df_patient_with_detection.groupBy( "医院ID","患者ID","OUT_ID","就诊类型" ).agg(  
                                        Func.min( col("检测日期").cast("int") ).alias("MIN_检测日期_INT") )
    
    
    logger.debug( df_patient_with_detection.count() )
    
    df_patient_std = df_temp_patient.join( df_patient_with_detection, on=["医院ID","患者ID","OUT_ID","就诊类型"],how="left" )\
                                    .withColumn("数据内容",  Func.when( ( col("MIN_检测日期_INT").isNotNull() ) & \
                                                                           ( ( col("标准处方日期_INT") - col("MIN_检测日期_INT"))<0  ), "检测前" )\
                                                                  .when( ( col("MIN_检测日期_INT").isNotNull() ) & \
                                                                        (  ( col("标准处方日期_INT")- col("MIN_检测日期_INT") )>0 ), "检测后" )\
                                                                  .when(   col("标准处方日期_INT") == col("MIN_检测日期_INT") , "检测当天" )\
                                                                  .otherwise("未检测") )
    
    

    # %%
    
    
    df_patient_tag = df_patient_std.drop_duplicates([c for c in df_patient_std.columns 
                                                                 if c in ["医院ID","患者ID","OUT_ID","就诊类型","心律不齐","其他心血管疾病",
                                                                        "脑血管疾病","神经系统疾病","高血糖","高血脂","肝功能异常","肾功能异常",
                                                                        "结缔组织病","COPD","哮喘","支气管扩张","恶性实体瘤","心衰","白细胞计数",
                                                                        "C反应蛋白","降钙素原","嗜肺军团菌",
                                                                        "肺炎衣原体","肺炎支原体","冠状病毒","合胞病毒","流感病毒","腺病毒",
                                                                        "柯萨奇病毒","鲍曼氏不动杆菌","大肠埃希菌","肺炎克雷伯菌","肺炎链球菌","金黄色葡萄球菌",
                                                                        "流感嗜血菌","嗜麦芽寡养单胞菌","嗜麦芽窄食单胞菌","铜绿假单胞菌","阴沟肠杆菌",
                                                                        "seg1_grp1","seg1_grp2","seg2_grp1","seg3_grp1","seg3_grp2","seg3_grp3"]])
    
    ## 采样次方式是为了经肯个
    tag_win = Window.partitionBy("uni_code")
    df_patient_tag = df_patient_tag.withColumn("pt_min_标准处方日期", Func.min( col("标准处方日期") ).over(tag_win)) \
                                   .where( col("标准处方日期") == col("pt_min_标准处方日期") ) \
                                   .drop_duplicates(["uni_code"])
    
    # 标准医保类型|标准性别|年龄|标准诊断|severe_case_after|标准科室
    df_patient_tag = df_patient_tag.withColumnRenamed("uni_code","pt_uni_code") \
                             .withColumnRenamed("标准医保类型","pt_标准医保类型") \
                             .withColumnRenamed("标准性别","pt_标准性别") \
                             .withColumnRenamed("年龄","pt_年龄") \
                             .withColumnRenamed("标准诊断","pt_标准诊断") \
                             .withColumnRenamed("标准科室","pt_标准科室")
    # patient_tag.show(1)
    # %%
    
    df_table_4_m = df_patient_std.groupBy(["医院ID","患者ID","OUT_ID","就诊类型","标准医保类型","标准性别","年龄","标准诊断",
                                 "severe_case","标准科室","formula","mole_comb","single_or_formula","SEQ","标准处方日期"])\
                                    .agg( Func.sum(col("金额")).alias("sales") ) \
                                 .withColumn("uni_code",Func.concat(col("医院ID"),col("患者ID"),col("OUT_ID"),col("就诊类型")))\
                                .withColumn("标准处方日期", col("标准处方日期").cast("int"))
    
    # table_4_m初始喹诺酮类换药
    df_quinolone_before = df_table_4_m.filter( (col("formula").rlike("氟喹诺酮") )
                                        & (col("SEQ") == "1"))
    
    
    win1 = Window.partitionBy("uni_code")
    df_quinolone_after = df_table_4_m.join( df_quinolone_before.select("uni_code").distinct(), on=["uni_code"], how="inner")
    
    df_quinolone_after = df_quinolone_after.withColumn("first_formula",  Func.lit("氟喹诺酮"))\
                                        .filter( ~( col("formula") == col("first_formula")) )\
                                        .withColumn("MIN_标准处方日期",  Func.min("标准处方日期").over(win1) )\
                                        .where(col("标准处方日期")==col("MIN_标准处方日期"))
    
    df_quinolone_after = df_quinolone_after.dropDuplicates( ["uni_code"])                        
    
    df_quinolone_before = df_quinolone_before.join(df_quinolone_after.select("uni_code"), on="uni_code", how="inner")
    
    df_quinolone_before = df_quinolone_before.dropDuplicates( ["uni_code"])
    
    # 将字段名换成对应的
    df_quinolone_before = df_quinolone_before.withColumnRenamed("severe_case","severe_case_before") \
                    .withColumnRenamed("formula","formula_before") \
                    .withColumnRenamed("mole_comb","mole_comb_before") \
                    .withColumnRenamed("single_or_formula","single_or_formula_before") \
                    .withColumnRenamed("SEQ","SEQ_before") \
                    .withColumnRenamed("标准处方日期","std_rx_date_before") \
                    .withColumnRenamed("sales","sales_before")
    
    # 为了防止字段重复·影响操作
    df_quinolone_before = df_quinolone_before.select("医院ID","患者ID","OUT_ID","就诊类型","severe_case_before","formula_before","mole_comb_before"
                                              ,"single_or_formula_before","SEQ_before","std_rx_date_before","sales_before")
    
    df_quinolone_after = df_quinolone_after.withColumnRenamed("severe_case","severe_case_after")\
                    .withColumnRenamed("formula","formula_after") \
                    .withColumnRenamed("mole_comb","mole_comb_after") \
                    .withColumnRenamed("single_or_formula","single_or_formula_after") \
                    .withColumnRenamed("SEQ","SEQ_after") \
                    .withColumnRenamed("标准处方日期","std_rx_date_after") \
                    .withColumnRenamed("sales","sales_after")
    
    # 这段具体只出现三次  貌似没用
    # mapping_inpatients_tag <- read_feather("L:/HIS Raw data/奥玛环素项目/07_标签/标签病人层面_0125.feather")
    # mapping_inpatients_tag <- table_2[,c(4:6,79,29:42,80,55:75,103)] %>% 
    #   distinct(患者ID, OUT_ID, .keep_all = T)
    
    df_quinolone_delivery = df_quinolone_after.join(df_quinolone_before, on=["医院ID","患者ID","OUT_ID","就诊类型"], how="left")
    
    df_quinolone_delivery = df_quinolone_delivery.join(df_patient_tag,["医院ID","患者ID","OUT_ID","就诊类型"],"left")

    # %%
    
    #初始头孢类换药
    df_cephalosporin_before = df_table_4_m.filter( (col("formula").rlike("头孢菌素类") )
                                        & (col("SEQ") == "1"))
    
    df_cephalosporin_after = df_table_4_m.join( df_cephalosporin_before.select("uni_code").distinct(), on=["uni_code"], how="inner")
    
    win2 = Window.partitionBy("uni_code")
    df_cephalosporin_after = df_cephalosporin_after.withColumn("first_formula",  Func.lit("头孢菌素类"))\
                                        .filter( ~( col("formula") == col("first_formula")) )\
                                        .withColumn("MIN_标准处方日期",  Func.min("标准处方日期").over(win2) )\
                                        .where(col("标准处方日期")==col("MIN_标准处方日期"))
    df_cephalosporin_after = df_cephalosporin_after.drop_duplicates(["uni_code"])
    
    df_cephalosporin_before = df_cephalosporin_before.join(df_cephalosporin_after.select("uni_code"), on="uni_code", how="inner")
    df_cephalosporin_before = df_cephalosporin_before.drop_duplicates(["uni_code"])
    
    # %%%%%%%%%%%%
    df_cephalosporin_before = df_cephalosporin_before.withColumnRenamed("severe_case","severe_case_before") \
                    .withColumnRenamed("formula","formula_before") \
                    .withColumnRenamed("mole_comb","mole_comb_before") \
                    .withColumnRenamed("single_or_formula","single_or_formula_before") \
                    .withColumnRenamed("SEQ","SEQ_before") \
                    .withColumnRenamed("标准处方日期","std_rx_date_before") \
                    .withColumnRenamed("sales","sales_before")
    
    df_cephalosporin_before = df_cephalosporin_before.select("医院ID","患者ID","OUT_ID","就诊类型","severe_case_before","formula_before","mole_comb_before",
                                               "single_or_formula_before","SEQ_before","std_rx_date_before","sales_before")
    
    df_cephalosporin_after = df_cephalosporin_after.withColumnRenamed("severe_case","severe_case_after")\
                    .withColumnRenamed("formula","formula_after") \
                    .withColumnRenamed("mole_comb","mole_comb_after") \
                    .withColumnRenamed("single_or_formula","single_or_formula_after") \
                    .withColumnRenamed("SEQ","SEQ_after") \
                    .withColumnRenamed("标准处方日期","std_rx_date_after") \
                    .withColumnRenamed("sales","sales_after")
    
    # ************
    df_cephalosporin_delivery = df_cephalosporin_after.join(df_cephalosporin_before,["医院ID","患者ID","OUT_ID","就诊类型"],"left")
    
    df_cephalosporin_delivery = df_cephalosporin_delivery.join(df_patient_tag,["医院ID","患者ID","OUT_ID","就诊类型"],"left")

    # %%
    
    # 进行判断重症医学科的操作
    # df_quinolone_delivery = df_quinolone_delivery.withColumn("std_dept.x",when(df_quinolone_delivery["标准科室"] == "重症医学科",
    #                                                                      "ICU").otherwise(col("标准诊断")))
    
    # df_cephalosporin_delivery = df_cephalosporin_delivery.withColumn("std_dept.x",when(df_cephalosporin_delivery["标准科室"] == "重症医学科",
    #                                                                              "ICU").otherwise(col("标准诊断")))
    # %%
    
    ## 判断检测前后是否换药
table4 = df_quinolone_delivery.select(["患者ID","就诊序号","就诊类型","标准医保类型","标准性别","年龄区间","标准诊断","标准科室",
                             "formula_before", "single_or_formula_before", "molecule_before", "sales_before", 
                             "formula_after", "single_or_formula_after", "molecule_after", "sales_after",
                             "检测前后是否换药", "嗜肺军团菌", "肺炎衣原体", "肺炎支原体", "冠状病毒", "合胞病毒", 
                             "流感病毒", "腺病毒", "柯萨奇病毒", "鲍曼氏不动杆菌", "大肠埃希菌", "肺炎克雷伯菌", 
                             "肺炎链球菌", "金黄色葡萄球菌", "流感嗜血菌", "嗜麦芽寡养单胞菌", "嗜麦芽窄食单胞菌", 
                             "铜绿假单胞菌", "阴沟肠杆菌"])

# "PATIENT_ID", "VISIT_ID", "VISIT_TYPE", "std_charge_type", "GENDER", "age_range", "std_diag", "severe_case", "std_dept",
    # %%
    
    df_quinolone_delivery.repartition(g_partition_num).write\
        .mode("overwrite").parquet( p_quinolone_result_dir)
    
    df_cephalosporin_delivery.repartition(g_partition_num).write\
        .mode("overwrite").parquet(p_cephalosporin_result_dir)
