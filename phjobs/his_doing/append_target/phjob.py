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
    
    p_main_dir = "s3://ph-origin-files/user/zazhao/2020年结果-csv/"
    
    ##输入数据目录
    p_patient_std_correlation = p_main_dir + "HIS_result/" + "correlation_data_result"
    
    ## 输出文件
    p_patient_with_more_target_out =  p_main_dir + "HIS_result/" + "patient_with_more_target_result"
    # %%
    
    ## 关联后的病人表数据
    df_patient_std_correlation = spark.read.parquet(p_patient_std_correlation)
    # %%
    
    ##  添加新的列
    df_patient_diagnois_target  = df_patient_std_correlation.withColumn("心律不齐",  when( col("诊断").\
                            rlike(r"心率失常|心律失常|心律不齐|心率不齐|心动过速|心动过缓|早搏|房室|QT|房颤|纤颤"), 1).otherwise(0) )\
                    .withColumn("心衰", when( col("诊断").rlike("心衰|心力衰竭"), 1 ).otherwise(0))\
                    .withColumn("其他心血管疾病", when( col("诊断").rlike("心功能|冠心病|冠状|动脉|心梗|心肌|心血管|心绞痛|心脏病"), 1 ).otherwise(0))\
                    .withColumn("脑血管疾病", when( col("诊断").rlike("脑梗|脑血管|中风|脑血栓|脑出血"), 1 ).otherwise(0))\
                    .withColumn("神经系统疾病", when( col("诊断").rlike("癫痫|EP|高颅压|颅内压增高|颅内高压|帕金森|阿尔兹海默|"+\
                                                                       "痴呆|神经炎|颅内感染|脑神经损害|脊神经|神经病|周围神经系统"), 1 ).otherwise(0))\
                    .withColumn("高血糖", when( col("诊断").rlike("高血糖"), 1 ).otherwise(0))\
                    .withColumn("高血压", when( col("诊断").rlike("高血压"), 1 ).otherwise(0))\
                    .withColumn("高血脂", when( col("诊断").rlike("高血脂|高脂|胆固醇"), 1 ).otherwise(0))\
                    .withColumn("肝功能异常", when( col("诊断").rlike("肝炎|肝损|肝功|肝硬|肝病|肝衰|肝纤维|药肝|脂肪肝"), 1 ).otherwise(0))\
                    .withColumn("肾功能异常", when( col("诊断").rlike("CRF|肾功|肾衰|肾病|透析|肾小管|肾小球|CAPD|尿毒|肾炎"), 1 ).otherwise(0))\
                    .withColumn("结缔组织病", when( col("诊断").rlike("结缔|风湿|关节炎"), 1 ).otherwise(0))\
                    .withColumn("COPD", when( col("诊断").rlike("COPD|慢性阻塞性肺|慢阻肺"), 1 ).otherwise(0))\
                    .withColumn("哮喘", when( col("诊断").rlike("哮喘|哮支"), 1 ).otherwise(0))\
                    .withColumn("支气管扩张", when( col("诊断").rlike("支气管扩张"), 1 ).otherwise(0))\
                    .withColumn("恶性实体瘤", when( ( col("诊断").rlike("癌|恶性肿瘤|恶性瘤|占位|放疗|化疗|CA|原位|转移|黑色素瘤") )
                                              &(col("诊断").rlike("CAPD|CAP")== False ) ,  1  ) .otherwise(0)) \
                    .withColumn("原始诊断字符数", Func.length( col("诊断") ) )

    # %%
    
    # 3. 标签列生成
    df_patient_more_target = df_patient_diagnois_target.withColumn("年龄区间",  Func.when( col("年龄")<8, lit("<8") )\
                                                      .when( (col("年龄") >=8)&(col("年龄") <=14), lit("8-14") )\
                                                      .when( (col("年龄") >=15)&(col("年龄") <=18), lit("15-18") )\
                                                      .when( (col("年龄") >=19)&(col("年龄") <=45), lit("19-15") )\
                                                      .when( (col("年龄") >45)&(col("年龄") <=65), lit("46-65") )\
                                                      .when( (col("年龄") >65), lit(">65") ) )\
                                                    .withColumn("混合感染", Func.when( ( col("鲍曼氏不动杆菌").contains("阳") )| \
                                                               (col("大肠埃希菌").contains("阳"))| (col("肺炎克雷伯菌").contains("阳"))| \
                                                               (col("肺炎链球菌").contains("阳"))| (col("金黄色葡萄球菌").contains("阳"))| \
                                                               (col("流感嗜血菌").contains("阳"))| (col("嗜麦芽寡养单胞菌").contains("阳"))| \
                                                               (col("嗜麦芽窄食单胞菌").contains("阳"))| (col("铜绿假单胞菌").contains("阳"))| \
                                                               (col("阴沟肠杆菌").contains("阳")),  10).otherwise(0)
                                                               )
    df_patient_more_target = df_patient_more_target.withColumn("混合感染", Func.when( ( col("冠状病毒").contains("阳") )| \
                                                               (col("合胞病毒").contains("阳"))| (col("流感病毒").contains("阳"))| \
                                                                 (col("腺病毒").contains("阳")), col("混合感染")+10 ).otherwise( col("混合感染") )  )  
    df_patient_more_target = df_patient_more_target.withColumn("混合感染", Func.when( ( col("肺炎支原体").contains("阳") )| \
                                                               (col("肺炎衣原体").contains("阳"))| (col("嗜肺军团菌").contains("阳")), \
                                                                  col("混合感染")+10 ).otherwise( col("混合感染") )  )  
    
    
    str_case = '严重感染|重型感染|重度肺炎|重型肺炎|重度呼吸|重度上呼吸|重型呼吸|'+\
         '重型上呼吸|重症肺炎|严重肺炎|重症呼吸|重症上呼吸|重度感染|重症感染|'+\
         '高危感染|危重感染|感染\\（重|感染\\（中重|感染\\（高|感染\\（危|炎\\（重|炎\\（中重|炎\\（高|'+\
         '炎\\（危|感染\\(重|感染\\(中重|感染\\(高|感染\\(危|炎\\(重|炎\\(中重|炎\\(高|炎\\(危'
    
    df_patient_more_target = df_patient_more_target.withColumn("severe_case", Func.when( col("诊断").rlike( str_case),"Y").otherwise("N"))

    # %%
    
    # 院内感染患者
    df_patient_std_pha = df_patient_more_target.filter( col("就诊类型")=="住院")\
                            .withColumn("标准处方日期", col("标准处方日期").cast("int"))\
                            .withColumn("标准入院时间", col("标准入院时间").cast("int"))
    df_patient_std_pha = df_patient_std_pha.select( ["医院ID",  "就诊类型", "患者ID","就诊序号","标准入院时间", "标准处方日期"] )
    
    ##### 初始未使用 抗菌药的患者
    # temp_data = data.withColumn("初始无抗菌药", Func.when( ( col("标准处方日期")-col("标准入院时间")<=2 ) & \
    #                                               ~col("MOLECULE_CATEGORY").isin(molde_class)
    #                                             , 1).otherwise(0) )
    # temp_data = temp_data.select("医院ID",  "就诊类型", "患者ID","就诊序号", "标准入院时间", "初始无抗菌药").distinct()
    
    # data = df_patient_correlation_std.join(temp_data, on=["医院ID",  "就诊类型", "患者ID","就诊序号", "标准入院时间" ], how="left")
    
    # data = data.withColumn("PHA患者", Func.when(  ((col("标准处方日期")-col("标准入院时间")) >2 ) & \
    #                                               (col("MOLECULE_CATEGORY").isin(molde_class) ) & \
    #                                               ( col("初始无抗菌药")==1 )
    #                                             , 1).otherwise(0) )
    # df_patient_std_5 = df_patient_correlation_std.join(data)
    
    
    # 筛选最小处方时间
    win_2 = Window.partitionBy("医院ID",  "就诊类型", "患者ID","就诊序号","标准入院时间")
    df_patient_std_pha = df_patient_std_pha.withColumn("MIN_标准处方日期", Func.min( col("标准处方日期") ).over(win_2))\
                .withColumn("HAP患者", Func.when((col("MIN_标准处方日期")-col("标准入院时间")>2),1 ).otherwise(0))\
                .drop("MIN_标准处方日期")
    
    df_patient_std_pha = df_patient_std_pha.select(["医院ID",  "就诊类型", "患者ID","就诊序号", "HAP患者"])
    # df_patient_std_pha.where(col("HAP患者")==1).show(1, vertical=True)
    # print(df_patient_std_pha.count())
    
    # df_patient_more_target = df_patient_more_target.join(df_patient_std_pha, on=["医院ID",  "就诊类型", "患者ID","就诊序号"], how="left" )
    # df_patient_more_target = df_patient_more_target.withColumn("HAP患者", Func.when( col("HAP患者").isNotNull(), col("HAP患者") ).otherwise(0) )
    # %%
    
    ## 6. 添加额外标签
    df_patient_more_target = df_patient_more_target.withColumn("细菌感染", Func.when( (col("鲍曼氏不动杆菌").contains("阳"))|(col("大肠埃希菌").contains("阳"))|\
                                                                     (col("肺炎克雷伯菌").contains("阳")) |(col("肺炎链球菌").contains("阳"))|\
                                                                     (col("金黄色葡萄球菌").contains("阳")) | (col("流感嗜血菌").contains("阳"))|\
                                                                     (col("嗜麦芽寡养单胞菌").contains("阳")) | (col("嗜麦芽窄食单胞菌").contains("阳"))|\
                                                                     (col("铜绿假单胞菌").contains("阳")) | (col("阴沟肠杆菌").contains("阳"))
                                                                ,1).otherwise(0) )\
                                    .withColumn("病毒感染", Func.when( (col("冠状病毒").contains("阳"))| (col("合胞病毒").contains("阳"))|\
                                                                     (col("流感病毒").contains("阳"))| (col("腺病毒").contains("阳")) 
                                                                ,1).otherwise(0) )\
                                    .withColumn("非典型病原菌感染", Func.when( (col("肺炎支原体").contains("阳"))| (col("肺炎衣原体").contains("阳")) | \
                                                                     (col("嗜肺军团菌").contains("阳"))
                                                                ,1).otherwise(0)) \
                                    .withColumn("seg1_grp1", Func.when( (col("年龄区间").contains("8-14")) |(col("年龄区间").contains("15-18") )
                                                                ,1).otherwise(0)) \
                                    .withColumn("seg1_grp2", Func.when( (col("神经系统疾病")==1 ) | (col("心律不齐")==1 ) |\
                                                                        (col("心衰")==1 )
                                                                ,1).otherwise(0)) \
                                    .withColumn("seg2_grp1", Func.when( (col("年龄区间").contains("8-14")) |(col("年龄区间").contains("15-18") )|\
                                                                        (col("心律不齐")==1 ) | (col("心衰")==1 )| \
                                                                        (col("神经系统疾病")==1 ) | (col("肝功能异常")==1 )| \
                                                                        (col("肾功能异常")==1 ) | (col("COPD")==1 )| \
                                                                        (col("恶性实体瘤")==1 ) 
                                                                ,1).otherwise(0)) \
                                    .withColumn("seg3_grp1", Func.when( ( (col("非典型病原菌感染")==1 ) & (col("细菌感染")==1 ) )|\
                                                                        ( (col("非典型病原菌感染")==1 ) & (col("病毒感染")==1 ) )
                                                                ,1).otherwise(0)) \
                                    .withColumn("seg3_grp2", Func.when( (col("嗜麦芽窄食单胞菌").contains("阳") ) |\
                                                                        (col("鲍曼氏不动杆菌").contains("阳") )| \
                                                                        (col("金黄色葡萄球菌").contains("阳") ) |
                                                                        (col("大肠埃希菌").contains("阳") ) 
                                                                ,1).otherwise(0)) \
                                    .withColumn("seg3_grp3", Func.when( col("seg3_grp2").isNull()
                                                                ,0).otherwise( col("seg3_grp2"))) 
                                    
    # df_patient_with_target_.show(1)

    # %%
    
    ## 保存关联后的结果
    df_patient_more_target = df_patient_more_target.repartition( g_partition_num)
    df_patient_more_target.write.format("parquet") \
        .mode("overwrite").save(p_patient_with_more_target_out)

