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
    g_project_name = kwargs['g_project_name']
    depend_job_names_keys = kwargs['depend_job_names_keys']
    g_partition_num = kwargs['g_partition_num']
    g_out_parameter = kwargs['g_out_parameter']
    g_out_dir = kwargs['g_out_dir']
    ### output args ###

    
    
    from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType
    from pyspark.sql.functions import col, date_format, count, isnull, lit
    from pyspark.sql.functions import when, isnan, udf, pandas_udf, PandasUDFType
    from pyspark.sql.window import Window
    from pyspark.sql import functions as Func
    from pyspark.sql import DataFrame, SparkSession    
    
    from typing import Iterator
    
    import pandas as pd
    import re        # %%
    # g_project_name = "HIS"
    # g_out_dir = "HIS_result"
    
    # result_path_prefix = get_result_path({"name":job_name, "dag_name":dag_name, "run_id":run_id})
    # depends_path = get_depends_path({"name":job_name, "dag_name":dag_name, 
    #                                  "run_id":run_id, "depend_job_names_keys":depend_job_names_keys })

    # %%
    ## ====== 输入文件和输出文件 ======
    
    # 
    g_whether_save_result = True
    
    
    p_main_dir = "s3://ph-origin-files/user/zazhao/2020年结果-csv/"
    
    # 清洗需要匹配的文件
    p_mapping_file = p_main_dir + "清洗规则/"
    p_drug_mapping = p_mapping_file + "raw_done.csv"
    p_dept_mapping = p_mapping_file + "科室清洗规则-重新划分Hanhui.csv"
    p_medical_insurance_mapping = p_mapping_file + "医保清洗规则.csv"
    # 上一布的输入文件
    p_input_patient = "s3://ph-origin-files/user/zazhao/2020年结果-csv/HIS_result/patient_simple_clean_out"
    # 输出文件
    p_out_main_dir = p_main_dir + "HIS_result/"
    p_patient_std_out = p_out_main_dir + "clean_data_result"
    

    # %%
    
    ## 读取病人数据
    df_raw_patient = spark.read.parquet(p_input_patient)
    # df_raw_patient.printSchema()

    # %%
    
    ## ============== 一. 确定样本医院 =================
    
    # df_sample_hospital = df_raw_patient.select(["医院ID","省份", "城市", "医院等级"]).distinct()
    
    # city_mapping = {
    #     "北京":"1","上海":"1","深圳":"1","广州":"1",
    #     "成都":"2","杭州":"2","重庆":"2","武汉":"2",
    #     "苏州":"2","西安":"2","天津":"2","南京":"2",
    #     "郑州":"2","长沙":"2","沈阳":"2","青岛":"2",
    #     "宁波":"2","东莞":"2","无锡":"2","昆明":"2",
    #     "大连":"2","厦门":"2","合肥":"2","佛山":"2",
    #     "福州":"2","哈尔滨":"2","济南":"2","温州":"2",
    #     "长春":"2","石家庄":"2","常州":"2","泉州":"2",
    #     "南宁":"2","贵阳":"2","南昌":"2","南通":"2",
    #     "金华":"2","徐州":"2","太原":"2","嘉兴":"2",
    #     "烟台":"2","惠州":"2","保定":"2","台州":"2",
    #     "中山":"2","绍兴":"2","乌鲁木齐":"2","潍坊":"2",
    #     "兰州":"2"
    # }
    
    # hosp_mapping = {
    #     "特级":"三级","三甲":"三级","二乙":"三级","三丙":"三级",
    #     "二甲":"二级","二乙":"二级","二丙":"二级",
    #     "甲":"一级","乙":"一级","丙":"一级"
    # }
    
    # @pandas_udf("string", PandasUDFType.SCALAR )
    # def change_city(x : Iterator[ pd.Series ] ):
    #     return x.apply(lambda i:  city_mapping[i] if i in city_mapping else "null" )
    
    # @pandas_udf("string", PandasUDFType.SCALAR )
    # def change_hosp(x : Iterator[ pd.Series ]):
    #     return x.apply(lambda i: hosp_mapping[i] if i in hosp_mapping else "null")
    
    
    # df_sample_hospital = df_sample_hospital.withColumn("城市等级",change_city( col("城市" ) ))
    # df_sample_hospital = df_sample_hospital.withColumn("新医院等级",change_hosp( col("医院等级") ))
    # df_sample_hospital = df_sample_hospital.select(["医院ID","省份", "城市", "医院等级", "城市等级","新医院等级"]).orderBy("医院ID")

    # %%
    
    ## ============== 诊断清洗 ======================
    
    ### 清洗诊断列
    @pandas_udf("string" , PandasUDFType.SCALAR)
    def pudf_standDiagnoise(x:pd.Series)->pd.Series:
        return x.apply(lambda i: changeDiagnoise(i))
    
    def changeDiagnoise(x):
        if x==None:
            new_x = "其他"
        elif re.findall(r".*(肺部感染|肺内感染|肺感染|支原体感染|衣原体感染).*", x):
            new_x = "肺部感染"
        elif re.findall( r"(肺炎|肺部炎症)", x):
            new_x = "肺炎"
        elif re.findall( r"(社区获得|CAP)", x) and (re.findall(r"CPAD", x)==list() ):
            new_x = "社区获得性肺炎"
        elif re.findall( r"(呼吸道感染|呼感)", x):
            new_x = "呼吸道感染"
        elif re.findall( r"(支气管肺炎)", x):
            new_x = "支气管肺炎"
        elif re.findall( r"(气管炎|急支|慢支|支气管周围炎|支炎)", x):
            new_x = "支气管炎"
        elif re.findall( r"(上感|上呼吸道感染)", x):
            new_x = "上呼吸道感染"
        elif re.findall( r"(扁桃体炎|扁桃体感染|扁桃体周围炎|化扁)", x):
            new_x = "扁桃体炎"
        elif re.findall( r"(咽炎|喉炎|咽峡炎|咽部感染|会厌炎)", x):
            new_x = "咽炎"
        elif re.findall( r"(流感|流行性感冒|甲流|乙流)", x):
            new_x = "流感"
        elif re.findall( r"(蜂窝织炎|蜂窝组织炎|丹毒|坏死性感染|化脓性感染|软组织感染|软组织炎)", x):
            new_x = "皮肤软组织感染"
        elif re.findall( r"(皮肤感染|皮炎|皮疹|湿疹|痤疮|毛囊炎|疖|外伤|烧伤|痈|疣)", x):
            new_x = "其他皮肤病"
        elif re.findall( r"(结膜炎|角膜炎|LASIK|睑板腺炎|睑腺炎|白内障|中耳炎|耳道炎|牙周炎|鼻炎|冠周炎|龈炎|睑缘炎|鼻窦炎)", x):
            new_x = "五官类疾病"
        elif re.findall( r"(胃炎|肠炎|食管炎|幽门螺杆菌感染|腹痛|阑尾炎|胆囊炎|胰腺炎|肠道感染|幽门螺旋杆菌|Hp感染|HP感染|腹泻)", x):
            new_x = "消化系统感染"
        elif re.findall( r"(泌尿系感染|尿路感染|尿道炎|前列腺炎|阴道炎|宫颈炎|尿道感染|尿路结石伴感染|盆腔炎)", x):
            new_x = "泌尿生殖系统感染"
        elif re.findall( r"(炎|感染)", x):
            new_x = "其他感染/炎症"
        elif re.findall( r"(呼吸困难|呼吸衰竭)", x):
            new_x = "呼吸困难"
        elif re.findall( r"(发烧|发热)", x):
            new_x = "发热"
        elif re.findall( r"(咳痰|有痰)", x):
            new_x = "咳痰"
        elif re.findall( r"(咳)", x):
            new_x = "咳嗽"
        elif re.findall( r"(感冒)", x):
            new_x = "普通感冒"
        elif re.findall( r"(咽痛|喉痛)", x):
            new_x = "咽痛"
        else:
            new_x = "其他"
        return new_x
        
    df_patient_diagnois =  df_raw_patient.withColumn("标准诊断", pudf_standDiagnoise(  col("诊断") )) 

    # %%
    
    ##  添加新的列
    df_patient_diagnois_target  = df_patient_diagnois.withColumn("心律不齐",  when( col("诊断").\
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
    
    ## ======================  清洗医保、科室清洗、医院ID ======================
    
    df_dept_mapping = spark.read.csv(p_dept_mapping,header=True).withColumnRenamed("std_dept", "标准科室")
    
    
    # 科室清洗
    df_patient_dept = df_patient_diagnois_target.join(df_dept_mapping, on=["科室"], how="left")
    # print("无法匹配到的科室  ", df_raw_patient.join(df_dept_mapping, on=["科室"], how="anti").count() )
    # left_anti， leftouter
    
    
    # 医保类型清洗
    
    df_medical_insurance_mapping = spark.read.csv(p_medical_insurance_mapping,header=True) \
                                            .withColumnRenamed("std_charge_type", "标准医保类型")
    # print(df_medical_insurance_mapping.columns)
    df_patient_std = df_patient_dept.join(df_medical_insurance_mapping,  on="医保类型", how="left")
    
    ## 为 Null的医保类型转换成 其他
    df_patient_std = df_patient_std.fillna("其他", subset=["医保类型", "标准医保类型"])
    # df_patient_std.select("标准医保类型").groupBy("标准医保类型").agg( Func.count("*").alias("样本数") ).show()
    # print("无法匹配到的医保类型:  ", df_patient_dept.join(df_medical_insurance_mapping,  on="医保类型", how="anti").count() )

    # %%
    # df_dept_mapping.show(1)
    ## 输出无法匹配的科室 
    # temp = df_raw_patient.join(df_dept_mapping, on=["科室"], how="anti").groupBy("科室").agg( Func.count("*").alias("样本数") )
    # temp.repartition(1).write.mode("overwrite").csv(p_out_main_dir+"无法匹配到的科室", sep=",", header="true", encoding="utf-8")
    
    # temp = df_patient_dept.join(df_medical_insurance_mapping,  on="医保类型", how="anti")
    # temp.groupBy("医保类型").agg( Func.count("*").alias("样本数") ).show()

    # %%
    
    # 性别清洗
    # df_patient.select("GENDER").distinct().show()
    df_patient_std = df_patient_std.withColumn("标准性别",  Func.when(   ~( col("性别")=="男") & ( ~(col("性别")=="女") ) |\
                                                               col("性别").isNull()
                                                               , "其他" ) .otherwise( col("性别"))  )

    # %%
    
    ####### 清洗后筛选
    
    #1、诊断清洗的筛查
    # 判断有空的
    df_patient_null_result =  df_patient_std.select([ Func.count( when(  Func.isnull(c)| col(c).isNull(), c ) ).alias(c) 
                                                for c in df_patient_std.columns])
    
    
    
    # 诊断特殊字符 及 空值
    df_temp = df_patient_std.where(    (col("心律不齐") == 0 ) & (col("其他心血管疾病") == 0 ) & \
                                    ( col( "脑血管疾病" ) == 0) & ( col("神经系统疾病") == 0 ) & 
                                    ( col("高血糖") == 0 ) & ( col("高血压") == 0 )& \
                                    ( col("高血脂") == 0 ) & ( col("肝功能异常") == 0) & \
                                    ( col("肾功能异常") == 0) & (col("结缔组织病") == 0) & \
                                    ( col("COPD") == 0) & (col("哮喘") == 0 )& \
                                    ( col("支气管扩张") == 0 )& ( col("恶性实体瘤") == 0 )& \
                                    ( col("标准诊断") == "其他" )&  (col("原始诊断字符数") <= 1) )
    
    # print( df_temp.count() )
    
    # 未清洗的数据
    ##患者层面未能清洗的数据
    
    ##医保
    
    ##科室

    # %%
    
    #  二、药品信息清洗-产品匹配
    
    # df_patient = df_patient.dropna()
    
    # 读入药品标准表
    df_drug_mapping = spark.read.csv(p_drug_mapping, header=True,encoding="gbk")
    df_drug_mapping = df_drug_mapping.withColumnRenamed("pfc", "PACK_ID")\
                                        .withColumnRenamed("brand", "BRAND")\
                                        .withColumnRenamed("molecule", "MOLECULE")\
                                        .withColumnRenamed("for", "FORM")\
                                        .withColumnRenamed("spec", "SPEC")\
                                        .withColumnRenamed("pack_number", "PACK_NUMBER")\
                                        .withColumnRenamed("manufacturer", "MANUFACTURER")
    # print(df_drug_mapping)
    # left 方式匹配
    df_patient_std = df_patient_std.join(df_drug_mapping, on=["药品名称", "规格", "剂型", "厂家"], how="left")
    # print("无法匹配到的产品名称  ", df_patient_std.join(df_drug_mapping, on=["药品名称", "规格", "剂型", "厂家"], how="anti").count() )

    # %% 
    
    # 三、 保存清洗后的数据
    df_patient_std = df_patient_std.repartition( g_partition_num )
    df_patient_std.write.format("parquet") \
        .mode("overwrite").save(p_patient_std_out)

