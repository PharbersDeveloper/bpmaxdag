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
    g_partition_csv = kwargs['g_partition_csv']
    g_partition_num = kwargs['g_partition_num']
    ### input args ###
    
    ### output args ###
    g_out_parameter = kwargs['g_out_parameter']
    ### output args ###

    from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType
    from pyspark.sql.functions import col, date_format, count, isnull
    from pyspark.sql.functions import when, isnan, udf, pandas_udf, PandasUDFType
    from pyspark.sql import functions as Func
    from pyspark.sql import DataFrame    
    
    from typing import Iterator
    
    import pandas
    import pandas as pd
    import re    
    # %%
    
    # ## 老方式连接集群,之后会取消
    # from pyspark.sql import SparkSession
    # # prepare
    # spark = SparkSession.builder \
    #     .master("yarn") \
    #     .appName("sshe write new_project in jupyter using python3") \
    #     .config("spark.sql.codegen.wholeStage", False) \
    #     .config("spark.sql.execution.arrow.pyspark.enabled", True) \
    #     .config("spark.driver.cores", "1") \
    #     .config("spark.driver.memory", "4g") \
    #     .config("spark.executor.cores", "1") \
    #     .config("spark.executor.memory", "4g") \
    #     .config("spark.executor.instances", "1") \
    #     .enableHiveSupport() \
    #     .getOrCreate()
    
    
    ## 新方式连接
    # spark

    # %%
    ## ====== 输入文件和输出文件 ======
    
    g_whether_save_result = True
    
    p_main_dir = "s3://ph-origin-files/user/zazhao/2020年结果-csv/"
    p_patient = p_main_dir + "病人"
    p_detection = p_main_dir + "检测"
    p_data_summary = p_main_dir +"条目数汇总表-2020.csv"
    
    p_out_main_dir = p_main_dir + "HIS_result/"
    p_check_result = p_out_main_dir + "check_result/"
    
    # check文件输出
    p_full_result =  p_check_result + "初查表-1-数据完整性"
    p_hospitalized_check_result_file = p_check_result + "初查表-2-住院"
    p_outpatient_check_result_file + p_check_result + "初查表-2-门诊"
    p_check_number = p_check_result+"初查表-3-条目数"
    p_check_all_people =  p_check_result+"初查表-3-总人次"
    p_check_money =  p_check_result+"初查表-3-金额"
    p_check_hospital_rate =  p_check_result+"初查表-4-住院比例"
    
    # 初清洗的病人数据
    p_patient_simple_clean_out = p_out_main_dir + "patient_simple_clean_out"

    # %% 
    
    ## 读取条目数汇总 
    df_summary_sample = spark.read.format("csv").load(p_data_summary, header=True)

    # %%
    
    ## 读取病人数据
    
    df_raw_patient = spark.read.csv( p_patient, header=True)
    
    old_col = ["省份", "城市", "医院等级", "就诊类型", "医院ID", "患者ID", "就诊序号", 
            "处方日期", "入院时间", "出院时间", "年龄", 
            "性别", "医保类型", "诊断", "科室", 
            "药品名称", "规格", "剂型", "厂家", "金额", "数量", "数量单位"]
    new_col = ["PROVINCE", "CITY", "HOSP_LEVEL", "TREAMENT_TYPE", "HOSP_ID", "PATIENT_ID", "VISIT_ID",
              "PRESCRIPTION_DATE", "ADMISSION_DATE" , "DISCHARGE_DATE",  "AGE", 
             "GENDER", "HIS_TYPE", "DIAGNOISE", "DEPT_NAME",
              "DRUG_NAME", "SPECIFICATION", "FORM", "MANUFACTURES", "MONEY", "NUMBER", "NUMBER_UNIT"]
    
    df_raw_patient = df_raw_patient.select(old_col)
    
    
    ## 去除字符串前后的空格,因为会影响到和其他表间进行匹配
    df_raw_patient = df_raw_patient.select([Func.trim(col(i)).alias(i)  for i in df_raw_patient.columns])
    
    ###################### 以下是需要转换列名为英文时才需要 
    # # 列名标准化
    # data_patient = data_patient.select( list( map( lambda x:col(x[0]).alias(x[1]),  zip(old_col, new_col) ) ))
    # # 转换日期格式
    # data_patient = data_patient.withColumn("PRESCRIPTION_DATE_STD", date_format("PRESCRIPTION_DATE", "yyyMM") )\
    #                                     .withColumn("ADMISSION_DATE_STD", date_format("ADMISSION_DATE", "yyyMM") )\
    #                                     .withColumn("DISCHARGE_DATE_STD", date_format("DISCHARGE_DATE", "yyyMM") )
    # ## 年龄转换成数字
    # df_patient = df_patient.withColumn("AGE", col("AGE").cast("int"))
    ######################
    
    ## 日期格式转换
    df_raw_patient = df_raw_patient.withColumn("标准处方日期", date_format("处方日期", "yyyyMMdd") )\
                                        .withColumn("标准入院时间", date_format("入院时间", "yyyyMMdd") )\
                                        .withColumn("标准出院时间", date_format("出院时间", "yyyyMMdd") )\
                                        .withColumn("年月", date_format("处方日期", "yyyyMM") )\
                                        .withColumn("年", date_format("处方日期", "yyyy") )\
                                        .withColumn("月", date_format("处方日期", "MM") )
    
    ## 年龄转换成数字
    df_raw_patient = df_raw_patient.withColumn("年龄", col("年龄").cast("int"))\
                        .withColumn("就诊序号", col("就诊序号").cast("int"))

    # %%
    
    ## 数据条目比较
    
    # sample_patient = df_raw_patient.groupby("医院ID").agg( Func.count("*")).orderBy("医院ID")
    # sample_patient.show()
    
    # df_summary_sample.show()
    
    # df_raw_detection.show(1, vertical=True)

    # %%
    
    ## 初查1-数据完整性
    def checkOne(df_data_patient, save_result=False):
        
        #### 计算 check-1
        # 统计存在空的字段
        df_null_sample_num =  df_data_patient.select([ Func.count( when(  Func.isnull(c)| col(c).isNull(), c ) ).alias(c) 
                                                for c in df_data_patient.columns])
        #df_null_sample_num.show(1, vertical=True)
    
        # df_all_count = df_data_patient.select([col(c).count().alias(c) for c in df_data_patient.columns])
        
        # 统计非空的字段 
        df_all_count = df_data_patient.select([Func.count(c).alias(c) for c in df_data_patient.columns])
    
        
        # 总的字段数
        samp_num = df_data_patient.count()
        
        # 空缺率
        df_missing_rate = df_null_sample_num.select( list(map( lambda x: (df_null_sample_num[x]/samp_num*100).alias(x),  df_null_sample_num.columns)))
    
    
        df_last = df_all_count.union(df_null_sample_num).union(df_missing_rate)
        
        if save_result==True:
            df_last.repartition( g_partition_csv ).write.mode("overwrite").csv(p_full_result, sep=",", header="true", encoding="utf-8")
        
            logger.debug("保存结果:  初查1-数据完整性")
    
    checkOne(df_data_patient= df_raw_patient, save_result=True)

    # %%
    
    ## 初查-2分子层面对比
    ### 初步清洗分子名称
    
    
    @pandas_udf("string", PandasUDFType.SCALAR)
    def pudf_change(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
        new_iter = iterator.apply( lambda x: changeSpecification(x) )
        return new_iter
    
    def changeSpecification(x ):
        # 处理读入的字符为空的情况
        if x==None:
            return "null"
        elif re.findall( r".*(莫西沙星).*", x):
            new_name = "莫西沙星"
        elif re.findall( r".*(左氧氟沙星).*", x):
            new_name = "左氧氟沙星"
        elif re.findall( r".*(头孢曲松).*", x):
            new_name = "头孢曲松"
        elif re.findall( r".*(阿奇霉素).*", x):
            new_name = "阿奇霉素"
        elif re.findall( r".*(多西环素).*", x):
            new_name = "多西环素"
        elif re.findall( r".*(米诺环素).*", x):
            new_name = "米诺环素"
        elif (re.findall( r".*(他唑巴坦|他唑邦坦|三唑巴坦|他唑巴).*", x)!=list()) \
                & ( re.findall( r".*(哌拉西林).*", x)!=list() ):
            new_name = "哌拉西林他唑巴坦纳"
        elif ( re.findall( r".*(哌拉西林).*", x)!=list() )\
                & ( re.findall( r".*(舒巴坦).*", x)!=list() ):
            new_name = "哌拉西林舒巴坦纳"
        elif re.findall( r".*(哌拉西林).*", x):
            new_name = "哌拉西林纳"
        elif ( re.findall( r".*(头孢哌酮).*", x)!=list() )\
                & ( re.findall( r".*(舒巴坦).*", x)!=list() ):
            new_name = "头孢哌酮钠舒巴坦钠"
        elif ( re.findall( r".*(头孢哌酮).*", x)!=list() )\
                & ( re.findall( r".*(他唑巴坦).*", x)!=list() ):
            new_name = "头孢哌酮钠他唑巴坦钠"
        elif re.findall( r".*(头孢哌酮).*", x):
            new_name = "头孢哌酮钠"
        elif ( re.findall( r".*(美洛西林).*", x)!=list() )\
                & ( re.findall( r".*(舒巴坦).*", x)!=list() ):
            new_name = "美洛西林钠舒巴坦钠"
        elif re.findall( r".*(美洛西林).*", x):
            new_name = "美洛西林钠"
        elif re.findall( r".*(依替米星).*", x):
            new_name = "依替米星"
        elif re.findall( r".*(头孢米诺).*", x):
            new_name = "头孢米诺"
        elif re.findall( r".*(替加环素).*", x):
            new_name = "替加环素"
        elif re.findall( r".*(头孢西丁).*", x):
            new_name = "头孢西丁"
        elif re.findall( r".*(头孢他啶).*", x):
            new_name = "头孢他啶"
        elif re.findall( r".*(厄他培南).*", x):
            new_name = "厄他培南"
        elif re.findall( r".*(利奈唑胺).*", x):
            new_name = "利奈唑胺"    
        elif re.findall( r".*(万古霉素).*", x):
            new_name = "万古霉素"
        elif ( re.findall( r".*(头孢噻肟).*", x)!=list()) & \
                ( re.findall( r".*(舒巴坦).*", x)!=list()):
            new_name = "头孢噻肟舒巴坦钠"
        elif re.findall( r".*(头孢噻肟).*", x):
            new_name = "头孢噻肟钠"
        elif re.findall( r".*(拉氧头孢).*", x):
            new_name = "拉氧头孢"
        elif re.findall( r".*(环丙沙星).*", x):
            new_name = "环丙沙星"
        else:
            new_name ="null"
        return new_name
    
    
    df_raw_patient = df_raw_patient.withColumn("标准分子名称", pudf_change( Func.col("药品名称") ) ) 
    
    # data_patient.persist()
    # df_raw_patient.show(1, vertical=True)

    # %%
    
    ## 分子层面比对
    
    # 就诊类型有哪些
    # data_patient.select("TREAMENT_TYPE").distinct().show()
    
    def unpivot(df, keys):
        # 功能：数据宽变长
        # 参数说明 df:dataframe,  keys 待转换表中需要保留的主键key，以list[]类型传入
        # 转换是为了避免字段类不匹配，统一将数据转换为string类型，如果保证数据类型完全一致，可以省略该句
        df = df.select(*[col(_).astype("string") for _ in df.columns])
        cols = [_ for _ in df.columns if _ not in keys]
        stack_str = ",".join(map(lambda x: "'%s', `%s`" % (x, x), cols))
        # feature, value 转换后的列名，可自定义
        df = df.selectExpr(*keys, "stack(%s, %s) as (feature, value)" % (len(cols), stack_str))
        return df
    
    
    def checkTwoMethod( data_patient, treament_type, df_drug_name_all=None):
        df_temp_patient = data_patient.where( data_patient["就诊类型"]==treament_type)
        
        df_temp_patient = df_temp_patient.select([ "医院ID", "就诊类型", "年月", "标准分子名称", "金额", "数量"])
        #df_temp_patient.select("年月").distinct().show()
    
        
        # 准备药品表
        #df_drug_name_all = df_temp_patient.select("标准分子名称").distinct().toPandas()["标准分子名称"].values.tolist()
        df_hosp_with_all_drug_pivot = df_temp_patient.where(col("标准分子名称").isNotNull() ).groupBy(["医院ID", "就诊类型", "年月" ] )\
                                    .pivot("标准分子名称" ).count()
        #df_hosp_with_all_drug_pivot.show(1, vertical=True)
        
        df_hosp_with_all_drug_pivot = df_hosp_with_all_drug_pivot.withColumnRenamed("医院ID", "HOSP_ID") \
                                                                .withColumnRenamed("就诊类型", "TREAMENT_TYPE") \
                                                                .withColumnRenamed("年月", "DATE") 
        df_hosp_with_all_drug = unpivot(df_hosp_with_all_drug_pivot, [ "HOSP_ID", "TREAMENT_TYPE", "DATE"] )
        df_hosp_with_all_drug = df_hosp_with_all_drug.withColumnRenamed( "HOSP_ID", "医院ID") \
                                                                .withColumnRenamed( "TREAMENT_TYPE", "就诊类型") \
                                                                .withColumnRenamed("DATE", "年月") 
        #df_hosp_with_all_drug.show(1,  vertical=True)
    
        
        df_hosp_with_all_drug = df_hosp_with_all_drug.withColumnRenamed("feature", "标准分子名称")\
                                    .withColumnRenamed("value", "条目数")
    
        
        # 统计药品的金额和数量
        df_part_one = df_temp_patient.groupby(["医院ID", "就诊类型", "年月", "标准分子名称", ]).agg( 
                                                    Func.count("*").alias("总条目数"), Func.sum( col("金额") ).alias("药品层面总金额"), 
                                                    Func.sum( col("数量") ).alias("药品层面总数量")     )
        # 统计医院层的金额和数量
        df_part_two = df_temp_patient.groupby(["医院ID", "就诊类型", "年月" ]).agg( 
                                          Func.sum( col("金额") ).alias("医院层面总金额"), Func.sum( col("数量") ).alias("医院层面总数量")  )
        
        # 医院层和药品层的表join
        df_part_three = df_part_one.join(df_part_two, on=["医院ID", "就诊类型", "年月"] , how="inner")
    
        df_part_four = df_part_three.withColumn("金额占比", col("药品层面总金额")/col("医院层面总金额") ) \
                                    .withColumn("数量占比", col("药品层面总数量")/col("医院层面总数量") )
     
        df_part_four = df_hosp_with_all_drug.join( df_part_four, on=["医院ID", "就诊类型", "年月", "标准分子名称"], how="left")
        
        df_part_four = df_part_four.orderBy(["医院ID", "年月", "药品层面总金额"], ascending=[1,1,0])
        #df_part_four.show(1, vertical=True)
    
        return df_part_four
    
    def checkTwo(data_patient_, save_result=False ):
        df_outpatient = checkTwoMethod(data_patient_, r"门诊")
        df_hospitalized = checkTwoMethod(data_patient_, r"住院")
    
        if save_result == True:
            df_outpatient =  df_outpatient.select(["医院ID", "就诊类型", "年月","标准分子名称", "总条目数", "药品层面总金额", 
                               "金额占比", "药品层面总数量", "数量占比"])
            
            df_outpatient.repartition(g_partition_csv).write.mode("overwrite").csv(p_outpatient_check_result_file, sep=",", header="true", encoding="utf-8")
            
            df_hospitalized =  df_hospitalized.select(["医院ID", "就诊类型", "年月","标准分子名称", "总条目数", "药品层面总金额", 
                               "金额占比", "药品层面总数量", "数量占比"])
    
            df_hospitalized.repartition(g_partition_csv).write.mode("overwrite").csv(p_hospitalized_check_result_file, sep=",", header="true", encoding="utf-8")
            
            logger.debug("保存结果:  分子层面比对")
    
    checkTwo(df_raw_patient, save_result = True)

    # %%
    
    ## 初查-3-4  医院数据连续性和稳定性 和 门诊住院比例 
    def checkThreeFour(df_patient, save_result=False):
        
        df_patient = df_patient.select(["省份", "城市", "医院等级", "就诊类型", "医院ID", "患者ID", "就诊序号", "处方日期", 
                                   "入院时间", "出院时间", "年月","年龄", "性别", "医保类型", "诊断", "科室", "药品名称", "规格", 
                                   "剂型", "厂家", "金额", "数量", "数量单位" ])
            
        # 医院数据连续性和稳定性
        df_patient = df_patient.withColumn("门诊",when(col("就诊类型") == "门诊",1))\
                                .withColumn("住院",when(col("就诊类型") == "住院",0))   
        df_patient = df_patient.select("医院ID","入院时间","数量单位","年月", "金额","患者ID",
                                  "就诊序号","就诊类型","门诊","住院").withColumn("金额", col("金额").cast("int"))
    
        year_month_list = ["201901","201902","201903","201904","201905","201906",
                                                   "201907","201908","201909","201910","201911","201912"]
        
        df_sample_table = df_patient.groupBy("医院ID").pivot("年月", year_month_list).count()\
                    .fillna(0).orderBy(["医院ID" ], ascending=[1])
        
        df_peopel_num_table = df_patient.groupBy("医院ID").pivot("年月", year_month_list).agg(count("患者ID")+count("就诊序号"))\
                    .fillna(0).orderBy(["医院ID"], ascending=[1])
        df_money_table = df_patient.groupBy("医院ID").pivot("年月", year_month_list).sum("金额")\
                    .fillna(0).orderBy(["医院ID"], ascending=[1])
        
        # 门诊住院比例 
        df_proportion = df_patient.groupBy("医院ID", "年月").agg(count(col("门诊")).alias("门诊总人数"), count(col("住院")).alias("住院总人数") )
        df_proportion = df_proportion.withColumn("radio", col("门诊总人数")/col("住院总人数")).orderBy(["医院ID", "年月"], ascending=[1,1])
        # df_proportion.show()
    
        
        if save_result==True:
            
            df_sample_table.repartition(g_partition_csv).write.mode("overwrite").csv(p_check_number, sep=",", header="true", encoding="utf-8")
            
            df_peopel_num_table.repartition(g_partition_csv).write.mode("overwrite").csv(p_check_all_people, sep=",", header="true", encoding="utf-8")
    
            df_money_table.repartition(g_partition_csv).write.mode("overwrite").csv(p_check_money, sep=",", header="true", encoding="utf-8")
    
            df_proportion.repartition(g_partition_csv).write.mode("overwrite").csv(p_check_hospital_rate, sep=",", header="true", encoding="utf-8")
            
            logger.debug("保存结果: 医院数据连续性和稳定性 和 门诊住院比例")
    
    checkThreeFour(df_raw_patient, save_result=True)

    # %%
    
    # IMS CHPA数据查询
    
    
    # MAX数据查询
    
    # df_raw_patient.show(2 )

    # %% 
    
    # 保存初步清理的处方表
    
    df_raw_patient = df_raw_patient.repartition(g_partition_num)
    df_raw_patient.write.format("parquet") \
        .mode("overwrite").save(p_patient_simple_clean_out)
