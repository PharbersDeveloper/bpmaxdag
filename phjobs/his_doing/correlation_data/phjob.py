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
    ### input args ###
    
    ### output args ###
    g_out_parameter = kwargs['g_out_parameter']
    ### output args ###

    
    
    from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructType
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
    p_mapping_file = p_main_dir+"清洗规则/"
    p_patient_target = p_mapping_file + "标签病人层面_测试用.csv"
    p_out_id_mapping = p_mapping_file+"门诊诊疗周期.csv"
    p_molecule_mapping = p_mapping_file + "20个分子分类.csv"
    # 输入目录
    p_patient_std_dir = "s3://ph-origin-files/user/zazhao/2020年结果-csv/HIS_result/clean_data_result"
    
    # 输出文件
    p_patient_std_correlation_out = p_main_dir+"HIS_result/" + "correlation_data_result"

    # %%
    
    ## 读取清洗后的处方数据
    df_patient_std = spark.read.parquet(p_patient_std_dir)

    # %%
    
    ## 读取检测数据
    df_raw_detection = spark.read.csv(p_detection, header=True)
    # df_raw_detection.show(1)
    
    df_raw_detection = df_raw_detection.select([ 'PATIENT_ID', 'VISIT_ID', 'ITEM_NAME', 'SUBJECT', 'REPORT_ITEM_NAME', 
                                                'RESULT', 'UNITS', 'ABNORMAL_INDICATOR', 'REQUESTED_DATE_TIME', 
                                                'RESULTS_RPT_DATE_TIME', 'DEPT_NAME'])
    
    df_raw_detection = df_raw_detection.withColumn("VISIT_ID", Func.col("VISIT_ID").cast("int"))\
                                        .withColumn("REQUESTED_DATE_TIME_STD",  date_format("REQUESTED_DATE_TIME", "yyyyMMdd")) \
                                        .withColumn("RESULTS_RPT_DATE_TIME_STD", date_format("RESULTS_RPT_DATE_TIME", "yyyyMMdd")) 
    # df_raw_detection.where( df_raw_detection["VISIT_ID"].isNull() ).count()

    # %%
    
    ##将数据标注区分 检测前 和 检测后
    logger.debug( df_raw_detection.count(), df_patient_std.count() )
    
    df_temp_detection = df_raw_detection.select(["PATIENT_ID", "VISIT_ID", "RESULTS_RPT_DATE_TIME_STD"  ] )\
                                            .withColumnRenamed("VISIT_ID", "就诊序号") \
                                            .withColumnRenamed("PATIENT_ID", "患者ID") \
                                            .withColumnRenamed("RESULTS_RPT_DATE_TIME_STD", "标准处方日期") \
                                            .withColumn("检测日期", col("标准处方日期").cast("int")  )
    ## 因为每个病人一天会有多个检测,所以需要去重
    df_temp_detection = df_temp_detection.distinct()
    # 就诊序号有空值存在
    df_temp_detection = df_temp_detection.where( col("就诊序号").isNotNull() )
    # 检测日期也有空值存在
    # print( df_temp_detection.where(  Func.isnull(col("检测日期")) ).count()   )
    df_detection_min_date = df_temp_detection.withColumn("最小检测日期", Func.min( col("检测日期") ).over(
                                                Window.partitionBy("患者ID", "就诊序号") ) )
    
    
    
    df_raw_patient = df_patient_std.withColumn("标准处方日期", col("标准处方日期").cast("int") )
    
    ##
    df_temp_patient = df_patient_std.join(df_detection_min_date,  on= ["患者ID", "就诊序号", "标准处方日期"], how="left")
    
    
    ## 此处以最小检测日期 区分检测前和 检测后
    df_temp_patient = df_temp_patient.withColumn("数据内容", Func.when( col("标准处方日期") < col("最小检测日期"), "检测前结果" )\
                                                      .when( col("标准处方日期") > col("最小检测日期"), "检测后结果" )\
                                                      .otherwise("未知") )
    logger.debug( df_temp_patient.count() )

    # %%
    
    # 1. 给病人打标签
    
    ## 原始为feather格式文件,转换为CSV格式
    
    df_target_patient_mapping = spark.read.csv(p_patient_target, header=True) \
                            .withColumnRenamed("PATIENT_ID", "患者ID")\
                            .withColumnRenamed("VISIT_ID", "就诊序号")
    
    
    df_target_patient_mapping = df_target_patient_mapping.withColumn("标准处方日期", date_format("处方日期", "yyyMMdd") )\
                                                            .withColumn("标准入院时间", date_format("入院时间", "yyyMMdd") )\
                                                            .withColumn("标准出院时间", date_format("出院时间", "yyyMMdd") ) \
                                                            .drop("处方日期", "入院时间", "出院时间")
    df_target_patient_mapping.printSchema()
    df_patient_std_1 = df_patient_std.join( df_target_patient_mapping, on=["医院ID","患者ID", "就诊序号", 
                                                                     "标准处方日期", "标准入院时间", "标准出院时间" ], how="left")
    
    
    logger.debug( "无法被打标签的病人样本:  ", df_patient_std.join( df_target_patient_mapping, on=["医院ID","患者ID", "就诊序号" ], how="anti").count() )
    # df_patient_target.show(2, vertical=True)
    # df_patient_with_target.show(2, vertical=True)
    # print(df_patient_target.columns)

    # %%
    
    # 2. 分子类别匹配
    df_molecule_class = spark.read.csv(p_molecule_mapping, header=True)\
                            .select(['分子名', 'Molecule', 'mole_category'])\
                            .withColumnRenamed("Molecule", "MOLECULE_OTHER")\
                            .withColumnRenamed("分子名", "MOLECULE")\
                            .withColumnRenamed("mole_category", "MOLECULE_CATEGORY")
    
    logger.debug( "无法被匹配到标准分子类别的:  ", df_patient_std_1.join(df_molecule_class, on="MOLECULE",how="anti" ).count() )
    df_patient_std_2 =  df_patient_std_1.join(df_molecule_class, on="MOLECULE",how="left" )
    # df_patient_with_mol_class.show(1, vertical=True)

    # %%
    
    # 3. 标签列生成
    df_patient_std_3 = df_patient_std_2.withColumn("年龄区间",  Func.when( col("年龄")<8, lit("<8") )\
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
    df_patient_std_3 = df_patient_std_3.withColumn("混合感染", Func.when( ( col("冠状病毒").contains("阳") )| \
                                                               (col("合胞病毒").contains("阳"))| (col("流感病毒").contains("阳"))| \
                                                                 (col("腺病毒").contains("阳")), col("混合感染")+10 ).otherwise( col("混合感染") )  )  
    df_patient_std_3 = df_patient_std_3.withColumn("混合感染", Func.when( ( col("肺炎支原体").contains("阳") )| \
                                                               (col("肺炎衣原体").contains("阳"))| (col("嗜肺军团菌").contains("阳")), \
                                                                  col("混合感染")+10 ).otherwise( col("混合感染") )  )  
    
    
    str_case = '严重感染|重型感染|重度肺炎|重型肺炎|重度呼吸|重度上呼吸|重型呼吸|'+\
         '重型上呼吸|重症肺炎|严重肺炎|重症呼吸|重症上呼吸|重度感染|重症感染|'+\
         '高危感染|危重感染|感染\\（重|感染\\（中重|感染\\（高|感染\\（危|炎\\（重|炎\\（中重|炎\\（高|'+\
         '炎\\（危|感染\\(重|感染\\(中重|感染\\(高|感染\\(危|炎\\(重|炎\\(中重|炎\\(高|炎\\(危'
    
    df_patient_std_3 = df_patient_std_3.withColumn("severe_case",Func.when( col("诊断").rlike( str_case),"Y").otherwise("N"))
    # %%
    
    ## 4. OUT_ID 匹配
    ################################################# 此处如何使用需要讨论
    df_out_id_mapping = spark.read.csv(p_out_id_mapping, header=True)
    
    # OUT_ID匹配  （对门诊部分14天为1诊疗周期的内容进行生成，代码未找到，但找到了结果文件，可直接进行匹配）
    # 门诊部分信息
    df_out_id_mapping = df_out_id_mapping.select(["HCODE", "PATIENT_ID", "VISIT_ID", "OUT_ID"]).distinct()
    df_out_id_mapping = df_out_id_mapping.select([ col("HCODE").alias("医院ID"),
                                                  col("PATIENT_ID").alias("患者ID"),
                                                  col("VISIT_ID").alias("就诊序号"),
                                                  col("OUT_ID")])
    
    ##########################################  注意此处需要讨论 具体是和 那个表匹配
    df_tag_out_id = df_target_patient_mapping.drop("OUT_ID")\
                        .join(df_out_id_mapping,on=["医院ID", "患者ID", "就诊序号"], how="left") 
    # 如果OUT_ID 为空就用 就诊序号代替            
    df_tag_out_id = df_tag_out_id.withColumn( "OUT_ID", when( col("OUT_ID").isNull() ,df_tag_out_id["就诊序号"]) \
                                                        .otherwise( col("OUT_ID") ) )
    #################################################            

    # %%
    
    ## 5. 筛选研究范围
    
    df_patient_std_4 =  df_patient_std_3.withColumn("uni_code",  Func.concat(col("医院ID"), col("患者ID") ) )
    
    
    # 使用替加环素的患者 
    ## 及只要使用过 替加环素的患者,就找出来
    data_part = df_patient_std_4.filter(col("molecule") == "替加环素").select("uni_code").distinct()
                
    # 筛选患者
    # 筛选目标分子  
    # 筛选目标剂型
    delivery_base = df_patient_std_4.filter(  col("标准诊断").rlike('社区获得性肺炎|肺部感染|呼吸道感染|支气管肺炎|肺炎'))\
                            .filter(col("MOLECULE_CATEGORY").isNotNull())\
                            .filter(~col("剂型").rlike('滴耳剂|眼用凝胶|滴眼剂|凝胶剂|软膏剂') )                           
    
    ## 过滤不含替加环素的患者 的数据
    delivery_base = delivery_base.join(data_part, on="uni_code", how="left_anti" )
    
    ## 是否需要筛选住院部分
    
    
    ## 是否需要 分类类别 重新定义
    # delivery_base = delivery_base.withColumn("mole_category",   Func.when(col("mole_category") == "环素类","四环素类") \
    #                                                                  .otherwise(col("mole_category")) )

    # %%
    
    ## 6. 添加额外标签
    df_patient_std = delivery_base.withColumn("细菌感染", Func.when( (col("鲍曼氏不动杆菌").contains("阳"))|(col("大肠埃希菌").contains("阳"))|\
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
    df_patient_std = df_patient_std.repartition(2)
    df_patient_std.write.format("parquet") \
        .mode("overwrite").save(p_patient_std_correlation_out)

