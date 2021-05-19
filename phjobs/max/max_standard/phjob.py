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
    g_project_name = kwargs['g_project_name']
    depend_job_names_keys = kwargs['depend_job_names_keys']
    g_max_path = kwargs['g_max_path']
    g_base_path = kwargs['g_base_path']
    g_out_dir = kwargs['g_out_dir']
    g_year = kwargs['g_year']
    g_month = kwargs['g_month']
    g_monthly_update = kwargs['g_monthly_update']
    ### input args ###
    
    ### output args ###
    a = kwargs['a']
    b = kwargs['b']
    ### output args ###

    
    
    
    
    
    from pyspark.sql import functions as func
    from pyspark.sql.functions import col
    from pyspark.sql.types import IntegerType, StringType, DoubleType, StructType, StructField    
    # %%
    # 测试用的参数
    
    # dag_name = 'Max'
    # run_id = 'max_test_beida_202012'
    # g_project_name ="贝达"
    # g_out_dir="202012_test"
    # result_path_prefix=get_result_path({"name":job_name, "dag_name":dag_name, "run_id":run_id})
    # depends_path=get_depends_path({"name":job_name, "dag_name":dag_name, 
    #                                  "run_id":run_id, "depend_job_names_keys":depend_job_names_keys })
    
    # # # g_monthly_update = 'False'
    # # # g_year = '2019'
    
    # g_monthly_update = 'True'
    # g_year = '2020'
    # g_month = '12'

    # %%
    # ========== 输入 输出 =========
    # 通用匹配文件
    p_product_map = g_max_path + "/" + g_project_name + "/" + g_out_dir + "/prod_mapping"
    
    
    # job-6 结果作为输入
    p_max_result = depends_path["max_city_result"]
    
    g_year = int(g_year)
    if g_monthly_update == 'True':
        g_month = int(g_month)
    
    # 输出
    p_max_standard =  result_path_prefix + "max_standard"
    p_max_standard_brief = result_path_prefix  + "max_standard_brief"

    # %%
    # p_max_result = p_max_result.replace("s3:", "s3a:")
    # p_max_standard = p_max_standard.replace("s3:", "s3a:")
    # p_max_standard_brief = p_max_standard_brief.replace("s3:", "s3a:")
    # %% 
    
    def createView(company, table_name, sub_path, other = "",
        time="2021-04-06",
        base_path = g_base_path):
             
            definite_path = "{base_path}/{sub_path}/TIME={time}/COMPANY={company}/{other}"
            path = definite_path.format(
                base_path = base_path,
                sub_path = sub_path,
                time = time,
                company = company,
                other = other
            )
            spark.read.parquet(path).createOrReplaceTempView(table_name)
            
    createView("PHARBERS", "city_normalize_mapping", "DIMENSION/MAPPING/CITY_NORMALIZE_MAPPING", time = "2021-04-14")        
    createView("PHARBERS", "universe_master_data", "DIMENSION/MAPPING/UNIVERSE_MASTER_DATA", time = "2021-04-14")
    createView("PHARBERS", "product_map_all_atc", "DIMENSION/MAPPING/PRODUCT_MAPPING_ALL_ATC", time = "2021-04-14")
    # %%
    # ========== 数据 mapping =========
    
    # mapping用文件：注意各种mapping的去重，唯一匹配
    
    # 1. 城市标准化
    city_normalize_mapping_sql = """  SELECT MAPPING_PROVINCE as PROVINCE, MAPPING_CITY as CITY, PROVINCE as PROVINCE_STD , CITY as CITY_STD
                                        FROM city_normalize_mapping """
    df_max_city_normalize =spark.sql(city_normalize_mapping_sql)
    df_max_city_normalize = df_max_city_normalize.select(["PROVINCE", "CITY", "PROVINCE_STD", "CITY_STD"])

    # %%
    # 2. df_master_data_map：PACK_ID - MOLECULE_STD - ACT, 无缺失
    universe_master_data_sql = """   SELECT * FROM universe_master_data  """
    df_master_data_map = spark.sql(universe_master_data_sql)
    
    df_pack_id_master_map = df_master_data_map.select("PACK_ID", "MOLE_NAME_CH", "PROD_NAME_CH", 
                                                      "CORP_NAME_CH", "DOSAGE", "SPEC", "PACK", "ATC4_CODE") \
                                        .distinct() \
                                        .withColumn("PACK_ID", df_master_data_map.PACK_ID.cast(IntegerType())) \
                                        .withColumnRenamed("MOLE_NAME_CH", "MOLE_NAME_CH_1") \
                                        .withColumnRenamed("ATC4_CODE", "ATC4_1")
    # 是否有重复
    num1 = df_pack_id_master_map.count()
    num2 = df_pack_id_master_map.dropDuplicates(["PACK_ID"]).count()
    logger.debug(num1 - num2)
    df_pack_id_master_map = df_pack_id_master_map.dropDuplicates(["PACK_ID"])
    # %%
    # 3. product_map_all_ATC: 有补充的新的 PACK_ID - 标准MOLECULE_STD - ACT （0是缺失）
    product_map_all_atc_sql = """  SELECT * FROM product_map_all_atc """
    df_molecule_act_map = spark.sql(product_map_all_atc_sql)
    
    
    df_molecule_act_map = df_molecule_act_map.select(["MIN", "MAPPING_MAMOLE_NAME", "PACK_ID", "PROJECT", "MOLE_NAME", "ATC2_CODE", "ATC3_CODE", "ATC4_CODE"])
    df_molecule_act_map = df_molecule_act_map.withColumnRenamed("MIN", "MIN_STD")\
                                              .withColumnRenamed("MAPPING_MAMOLE_NAME", "MOLECULE_STD")\
                                              .withColumnRenamed( "MOLE_NAME", "MOLE_NAME_CH")
    
    
    # ATC4_CODE 列如果是0，用ATC3_CODE补充，ATC3_CODE 列也是0，用ATC2_CODE补充
    df_molecule_act_map = df_molecule_act_map.withColumn('ATC4_CODE', func.when(df_molecule_act_map.ATC4_CODE == '0', 
                                                                                func.when(df_molecule_act_map.ATC3_CODE == '0', 
                                                                                    df_molecule_act_map.ATC2_CODE) \
                                                                              .otherwise(df_molecule_act_map.ATC3_CODE) ) \
                                                             .otherwise(df_molecule_act_map.ATC4_CODE))
    
    
    df_add_pack_id = df_molecule_act_map.where(df_molecule_act_map.PROJECT == g_project_name) \
                    .select("MIN_STD", "PACK_ID").distinct() \
                    .withColumn("PACK_ID", df_molecule_act_map.PACK_ID.cast(IntegerType()))
        
    df_add_pack_id = df_add_pack_id.withColumn("PACK_ID", func.when(df_add_pack_id.PACK_ID == "0", None) \
                    .otherwise(df_add_pack_id.PACK_ID)) \
                    .withColumnRenamed("PACK_ID", "PACK_ID_ADD") 
    
    # df_add_pack_id.show(1, vertical=True)
    
    df_molecule_act_map = df_molecule_act_map.select("MOLECULE_STD", "MOLE_NAME_CH", "ATC4_CODE") \
                    .withColumnRenamed("MOLE_NAME_CH", "MOLE_NAME_CH_2") \
                    .withColumnRenamed("ATC4_CODE", "ATC4_2") \
                    .dropDuplicates(["MOLECULE_STD"])
    
    
    df_molecule_act_map = df_molecule_act_map.withColumn("MOLE_NAME_CH_2", 
                        func.when(df_molecule_act_map.MOLE_NAME_CH_2 == "0", None)\
                            .otherwise(df_molecule_act_map.MOLE_NAME_CH_2)) \
                            .withColumn("ATC4_2", 
                                        func.when(df_molecule_act_map.ATC4_2 == "0", None)\
                                        .otherwise(df_molecule_act_map.ATC4_2))
    # %%
    # 4. 产品信息，列名标准化
    # product_map = spark.read.parquet(p_product_map)
    
    #### 读如产品匹配表  product-map
    df_product_map = spark.read.parquet(p_product_map)
    
    # a. 列名清洗统一
    # 有的min2结尾有空格与无空格的是两条不同的匹配
    for i in df_product_map.columns:
        if i in ["标准通用名", "通用名_标准", "药品名称_标准", "S_Molecule_Name"]:
            df_product_map = df_product_map.withColumnRenamed(i, "通用名")
        if i in ["min1_标准"]:
            df_product_map = df_product_map.withColumnRenamed(i, "min2")
        if i in ["packcode", "Pack_ID", "Pack_Id", "PackID", "packid"]:
            df_product_map = df_product_map.withColumnRenamed(i, "pfc")
        if i in ["商品名_标准", "S_Product_Name"]:
            df_product_map = df_product_map.withColumnRenamed(i, "标准商品名")
        if i in ["剂型_标准", "Form_std", "S_Dosage"]:
            df_product_map = df_product_map.withColumnRenamed(i, "标准剂型")
        if i in ["规格_标准", "Specifications_std", "药品规格_标准", "S_Pack"]:
            df_product_map = df_product_map.withColumnRenamed(i, "标准规格")
        if i in ["包装数量2", "包装数量_标准", "Pack_Number_std", "S_PackNumber", "最小包装数量"]:
            df_product_map = df_product_map.withColumnRenamed(i, "标准包装数量")
        if i in ["标准企业", "生产企业_标准", "Manufacturer_std", "S_CORPORATION", "标准生产厂家"]:
            df_product_map = df_product_map.withColumnRenamed(i, "标准生产企业")
    if g_project_name == "Janssen" or g_project_name == "NHWA":
        if "标准剂型" not in df_product_map.columns:
            df_product_map = df_product_map.withColumnRenamed("剂型", "标准剂型")
        if "标准规格" not in df_product_map.columns:
            df_product_map = df_product_map.withColumnRenamed("规格", "标准规格")
        if "标准生产企业" not in df_product_map.columns:
            df_product_map = df_product_map.withColumnRenamed("生产企业", "标准生产企业")
        if "标准包装数量" not in df_product_map.columns:
            df_product_map = df_product_map.withColumnRenamed("包装数量", "标准包装数量")
    
    # 列名标准化
    df_product_map = df_product_map.withColumnRenamed('min1', 'MIN') \
                        .withColumnRenamed('min2', 'MIN_STD') \
                        .withColumnRenamed('通用名', 'MOLECULE_STD') \
                        .withColumnRenamed('标准商品名', 'BRAND_STD') \
                        .withColumnRenamed('标准剂型', 'FORM_STD') \
                        .withColumnRenamed('标准规格', 'SPECIFICATIONS_STD') \
                        .withColumnRenamed('标准包装数量', 'PACK_NUMBER_STD') \
                        .withColumnRenamed('标准生产企业', 'MANUFACTURER_STD') \
                        .withColumnRenamed('标准集团', 'CORP_STD') \
                        .withColumnRenamed('标准途径', 'ROUTE_STD') \
                        .withColumnRenamed('pfc', 'PACK_ID')
    df_product_map = df_product_map.withColumn('PACK_NUMBER_STD', 
                                col('PACK_NUMBER_STD').cast(IntegerType()) ) \
                            .withColumn('PACK_ID', 
                                col('PACK_ID').cast(IntegerType()))
    
    df_product_map = df_product_map.select([col(i).alias(i.upper() )  for i in df_product_map.columns])
    
    
    # b. 选取需要的列
    df_product_map = df_product_map \
                    .select("MIN_STD", "PACK_ID", "MOLECULE_STD", "BRAND_STD", "FORM_STD", "SPECIFICATIONS_STD", "PACK_NUMBER_STD", "MANUFACTURER_STD") \
                    .withColumn("PACK_ID", df_product_map["PACK_ID"].cast(IntegerType())) \
                    .withColumn("PACK_NUMBER_STD", df_product_map["PACK_NUMBER_STD"].cast(IntegerType())) \
                    .distinct()
    
    
    # c. PACK_ID为0统一替换为null
    df_product_map = df_product_map.withColumn("PACK_ID", func.when(df_product_map.PACK_ID == 0, None).otherwise(df_product_map.PACK_ID)).distinct()
    df_product_map = df_product_map.withColumn("PROJECT", func.lit(g_project_name)).distinct()
    
    # d. MIN_STD处理
    df_product_map = df_product_map \
                    .withColumn("MIN_STD", func.regexp_replace("MIN_STD", "&amp;", "&")) \
                    .withColumn("MIN_STD", func.regexp_replace("MIN_STD", "&lt;", "<")) \
                    .withColumn("MIN_STD", func.regexp_replace("MIN_STD", "&gt;", ">"))
    
    
    # e. 补充PACK_ID
    df_product_map = df_product_map.join(df_add_pack_id, on="MIN_STD", how="left")
    df_product_map = df_product_map.withColumn("PACK_ID", 
                            func.when((df_product_map.PACK_ID.isNull()) & (~df_product_map.PACK_ID_ADD.isNull()), 
                            df_product_map.PACK_ID_ADD).otherwise(df_product_map.PACK_ID)) \
                            .drop("PACK_ID_ADD")
    
    # f. 去重：保证每个MIN_STD只有一条信息, dropDuplicates会取first
    df_product_map = df_product_map.dropDuplicates(["MIN_STD"])
    #                                 .drop("MOLECULE_STD")

    # %%
    # 读取max 的结果
    # df_max_result = spark.read.parquet(p_max_result )
    struct_type_max_result = StructType([  StructField('PROVINCE', StringType(), True),
                                            StructField('CITY', StringType(), True),
                                            StructField('DATE', IntegerType(), True),
                                            StructField('MIN_STD', StringType(), True),
                                            StructField('MOLECULE', StringType(), True),
                                            StructField('PANEL', DoubleType(), True),
                                            StructField('MARKET', StringType(), True),
                                            StructField('PREDICT_SALES', DoubleType(), True),
                                            StructField('PREDICT_UNIT', DoubleType(), True)  ] )
    df_max_result = spark.read.format("parquet").load(p_max_result, schema=struct_type_max_result)
    df_max_result =  df_max_result.withColumnRenamed('MIN_STD', 'MIN_STD_MAX') 
    
    if g_monthly_update == 'True':
        df_max_result = df_max_result.where(col('DATE') == (g_year*100 + g_month))
    elif g_monthly_update == 'False':
        df_max_result = df_max_result.where((col('DATE')/100).cast(IntegerType()) == g_year)

    # %%
    # =========== 数据执行 =============
    # 杨森6月的max结果 衡水市- 湖北省 错误，先强制改为衡水市- 河北省
    if g_project_name == "Janssen":
        df_max_result = df_max_result.withColumn("PROVINCE", func.when(df_max_result.CITY == "衡水市", func.lit("河北省")) \
                                                    .otherwise(df_max_result.PROVINCE))
    
    # 1. df_max_result 的 Prod_Name（MIN_STD） 处理
    df_max_result = df_max_result.withColumn("MIN_STD_tmp", col("MIN_STD_MAX"))
    df_max_result = df_max_result.withColumn("MIN_STD_tmp", func.regexp_replace("MIN_STD_tmp", "&amp;", "&")) \
                        .withColumn("MIN_STD_tmp", func.regexp_replace("MIN_STD_tmp", "&lt;", "<")) \
                        .withColumn("MIN_STD_tmp", func.regexp_replace("MIN_STD_tmp", "&gt;", ">"))
    if g_project_name == "Servier":
        df_max_result = df_max_result.withColumn("MIN_STD_tmp", func.regexp_replace("MIN_STD_tmp", "阿托伐他汀\\+齐鲁制药\\(海南\\)有限公司", "美达信"))
    if g_project_name == "NHWA":
        df_max_result = df_max_result.withColumn("MIN_STD_tmp", func.regexp_replace("MIN_STD_tmp", "迪施宁乳剂", "迪施乐乳剂"))
    
    # 2. df_product_map 匹配 MIN_STD ：获得 PACK_ID, MOLECULE_STD, BRAND_STD, FORM_STD, SPECIFICATIONS_STD, PACK_NUMBER_STD, MANUFACTURER_STD
    ############################################### 原脚本如下,修改后的在下面
    df_max_standard = df_max_result.join(df_product_map, df_max_result["MIN_STD_tmp"] == df_product_map["MIN_STD"], how="left") \
                        .drop("MIN_STD","MIN_STD_tmp")
    
    
    # 3. df_pack_id_master_map 匹配 PACK_ID ：获得 MOLE_NAME_CH_1, ATC4_1, PROD_NAME_CH, "CORP_NAME_CH, DOSAGE, SPEC, PACK
    df_max_standard = df_max_standard.join(df_pack_id_master_map, on=["PACK_ID"], how="left")
    # print( df_max_standard )
    
    # 4. df_molecule_act_map 匹配 MOLECULE_STD：获得 MOLE_NAME_CH_2, ATC4_2
    df_max_standard = df_max_standard.join(df_molecule_act_map, on=["MOLECULE_STD"], how="left")

    # %%
    # 5. 整合 master 匹配结果 和 df_product_map, df_molecule_act_map 匹配结果
    '''
    ATC4_1 和 MOLE_NAME_CH_1 来自 master 有 pack_id 匹配得到 ; ATC4_2 和 MOLE_NAME_CH_2 来自 df_molecule_act_map 
    '''
    # A10C/D/E是胰岛素, MOLECULE_STD和公司名用master, 其他信息用df_product_map
    df_max_standard = df_max_standard.withColumn("ATC", func.when(df_max_standard["ATC4_1"].isNull(), df_max_standard["ATC4_2"]) \
                                            .otherwise(df_max_standard["ATC4_1"]))
    
    df_max_standard_yidaosu = df_max_standard.where(func.substring(df_max_standard.ATC, 0, 4).isin(['A10C', 'A10D', 'A10E'])) \
                            .withColumn("PROD_NAME_CH", df_max_standard['BRAND_STD']) \
                            .withColumn("DOSAGE", df_max_standard['FORM_STD']) \
                            .withColumn("SPEC", df_max_standard['SPECIFICATIONS_STD']) \
                            .withColumn("PACK", df_max_standard['PACK_NUMBER_STD'])
    
    df_max_standard_others = df_max_standard.where((~func.substring(df_max_standard.ATC, 0, 4).isin(['A10C', 'A10D', 'A10E'])) \
                                             | df_max_standard.ATC.isNull())
    
    # 合并 df_max_standard_yidaosu 和 df_max_standard_others
    df_max_standard = df_max_standard_others.union(df_max_standard_yidaosu.select(df_max_standard_others.columns))
    
    
    # master 匹配不上的(ATC4_1是null)c用 df_molecule_act_map 和 df_product_map 信息
    df_max_standard = df_max_standard.withColumn("MOLECULE_STD_MASTER", func.when(df_max_standard["MOLE_NAME_CH_1"].isNull(), 
                                                                         df_max_standard["MOLE_NAME_CH_2"]) \
                                            .otherwise(df_max_standard["MOLE_NAME_CH_1"])) \
                            .withColumn("BRAND_STD", func.when(df_max_standard["ATC4_1"].isNull(), df_max_standard["BRAND_STD"]) \
                                            .otherwise(df_max_standard["PROD_NAME_CH"])) \
                            .withColumn("FORM_STD", func.when(df_max_standard["ATC4_1"].isNull(), df_max_standard["FORM_STD"]) \
                                            .otherwise(df_max_standard["DOSAGE"])) \
                            .withColumn("SPECIFICATIONS_STD", func.when(df_max_standard["ATC4_1"].isNull(), df_max_standard["SPECIFICATIONS_STD"]) \
                                            .otherwise(df_max_standard["SPEC"])) \
                            .withColumn("PACK_NUMBER_STD", func.when(df_max_standard["ATC4_1"].isNull(), df_max_standard["PACK_NUMBER_STD"]) \
                                            .otherwise(df_max_standard["PACK"])) \
                            .withColumn("MANUFACTURER_STD", func.when(df_max_standard["ATC4_1"].isNull(), df_max_standard["MANUFACTURER_STD"]) \
                                            .otherwise(df_max_standard["CORP_NAME_CH"])) \
                            .drop("ATC4_1", "ATC4_2", "MOLE_NAME_CH_1", "MOLE_NAME_CH_2")
    
    # 没有标准MOLECULE_STD的 用原始的MOLECULE_STD
    df_max_standard = df_max_standard.withColumn("MOLECULE_STD_MASTER", func.when(df_max_standard['MOLECULE_STD_MASTER'].isNull(), 
                                                                         df_max_standard['MOLECULE_STD']) \
                                                        .otherwise(df_max_standard['MOLECULE_STD_MASTER']))
    
    # CITY 标准化：
    '''
    先标准化省，再用(标准省份-CITY)标准化市
    '''
    df_max_standard = df_max_standard.join(df_max_city_normalize.select("PROVINCE", "PROVINCE_STD").distinct(), on=["PROVINCE"], how="left")
    df_max_standard_all = df_max_standard.join(df_max_city_normalize.select("CITY", "PROVINCE_STD", "CITY_STD").distinct(),
                            on=["PROVINCE_STD", "CITY"], how="left")
    

    # %%
    # 全量结果汇总
    df_max_standard_all = df_max_standard_all.withColumn("PROJECT", func.lit(g_project_name))
    
    df_max_standard_all = df_max_standard_all.select("PROJECT", "PROVINCE", "CITY" ,"DATE",
                                               "MIN_STD_MAX", "MOLECULE", "PANEL", 
                                               "MARKET", "Predict_Sales", "Predict_Unit", 
                                               "MOLECULE_STD_MASTER", "BRAND_STD", "FORM_STD", 
                                               "SPECIFICATIONS_STD", "PACK_NUMBER_STD", "MANUFACTURER_STD",
                                               "PROVINCE_STD", "CITY_STD", "PACK_ID", "ATC")
    df_max_standard_all = df_max_standard_all.withColumn("DATE", df_max_standard_all["DATE"].cast('int') )
    
    
    # MIN_STD_MAX 和 MOLECULE_STD_MASTER 是中间列名，在保存的时候使用标准列名会更合适
    df_max_standard_all = df_max_standard_all.withColumn("DATE_COPY", df_max_standard_all.DATE)\
                    .withColumnRenamed("MIN_STD_MAX", "MIN_STD") \
                    .withColumnRenamed("MOLECULE_STD_MASTER","MOLECULE_STD" )
    
    
        
    # 目录结果汇总,
    df_max_standard_brief = df_max_standard_all.select("PROJECT", "DATE", "MOLECULE_STD", "ATC",
                                                 "MARKET", "PACK_ID").distinct()

    # %%
    # =========== 数据输出 =============
    # 根据日期分桶写出
    df_max_standard_all = df_max_standard_all.repartition(1)
    df_max_standard_all.write.format("parquet").partitionBy("DATE_COPY") \
    .mode("append").save(p_max_standard)
    
    # 输出brief结果
    df_max_standard_brief = df_max_standard_brief.repartition(1)
    df_max_standard_brief.write.format("parquet").partitionBy("DATE") \
    .mode("append").save(p_max_standard_brief)

    # %%
    
    # # manual__2021-05-06T09_58_09.076641+00_00 为贝达 2020年12月的 月更数据
    # temp_path = "s3a://ph-max-auto/2020-08-11/Max_test2/refactor/runs/manual__2021-05-06T09_58_09.076641+00_00/"
    # name = "max_standard/max_standard/"
    # df_hos_1 = spark.read.parquet( temp_path+name \
    #                               +"")
    # print(df_hos_1.count() )
    
    # df_hos_2 = df_max_standard_all
    # df_hos_1.agg(func.sum('PREDICT_SALES'),func.sum('PREDICT_UNIT')).show()
    # df_hos_2.agg(func.sum('PREDICT_SALES'),func.sum('PREDICT_UNIT')).show()
    # print( df_hos_2.count() )

    # %%
    # ### 数据校准
    # p_result_maxdata_standard = "s3a://ph-stream/common/public/max_result/0.0.5/max_standard/贝达_max_standard/"
    # df_result_maxdata_standard = spark.read.parquet( p_result_maxdata_standard )
    # df_result_maxdata_standard.persist()
    
    
    
    # print( df_max_standard_all.columns)
    # print( df_result_maxdata_standard.columns )
    
    # print( len( df_max_standard_all.columns) )
    # print( len(  df_result_maxdata_standard.columns ) )
    
    
    
    
    # a = ['PROJECT', 'PROVINCE', 'CITY', 'DATE', 'MIN_STD', 'MOLECULE', 'PANEL', 'MARKET', 'Predict_Sales', 'Predict_Unit', 
    #      'MOLECULE_STD', 'BRAND_STD', 'FORM_STD', 'SPECIFICATIONS_STD', 'PACK_NUMBER_STD', 'MANUFACTURER_STD', 'PROVINCE_STD', 
    #      'CITY_STD', 'PACK_ID', 'ATC', 'DATE_COPY']
    # b = ['project', 'Province', 'City', 'Date', 'Prod_Name', 'Molecule', 'PANEL', 'DOI', 'Predict_Sales', 'Predict_Unit', 
    #      '标准通用名', '标准商品名', '标准剂型', '标准规格', '标准包装数量', '标准生产企业', '标准省份名称', '标准城市名称', 'PACK_ID', 
    #      'ATC', 'Date_copy']
    # df_result_maxdata_standard =  df_result_maxdata_standard.select([col(i).alias(j)   for i,j in zip(b,a)])
    # print( df_result_maxdata_standard.columns )
    
    
    # # 列长度一样
    # len(df_result_maxdata_standard.columns) == len( df_max_standard_all.columns )
    # # df_result_maxdata_standard.show(1, vertical=True)
    
    # # 日期数目不同
    # # df_result_maxdata_standard: 49个月
    # # df_max_standard_all: 48 个月
    # df_result_maxdata_standard.select("DATE").distinct().show()
    # df_max_standard_all.select("DATE").distinct().show()
    # print( df_result_maxdata_standard.select("DATE").distinct().count(), df_max_standard_all.select("DATE").distinct().count() )
    
    
    
    # # df_max_standard_all.show(1, vertical=True)
    # ## 月份数目不同，所以总的数据量不一致
    # # print("result   lines number: ", df_result_maxdata_standard.count() )
    # # print("raw data lines number: ", df_max_standard_all.count() )
    # # ## 限定月份数目
    # # print("result   lines number: ", df_result_maxdata_standard.where( (df_result_maxdata_standard.Date >=202001)& ((df_result_maxdata_standard.Date < 202101) )   ).count() )
    # # print("raw data lines number: ", df_max_standard_all.where( (df_max_standard_all.DATE >=202001) & ((df_max_standard_all.DATE < 202101) )   ).count() )
    # # print("result   lines number: ", df_result_maxdata_standard.where( (df_result_maxdata_standard.Date >=201901)& ((df_result_maxdata_standard.Date < 202001) )   ).count() )
    # # print("raw data lines number: ", df_max_standard_all.where( (df_max_standard_all.DATE >=201901) & ((df_max_standard_all.DATE < 202001) )   ).count() )
    # # print("result   lines number: ", df_result_maxdata_standard.where( (df_result_maxdata_standard.Date >=201801)& ((df_result_maxdata_standard.Date < 201901) )   ).count() )
    # # print("raw data lines number: ", df_max_standard_all.where( (df_max_standard_all.DATE >=201801) & ((df_max_standard_all.DATE < 201901) )   ).count() )
    
    # # print("result   lines number: ", df_result_maxdata_standard.where( (df_result_maxdata_standard.DATE >=202001)& \
    # #                                                                   ((df_result_maxdata_standard.DATE < 202101) )   ).groupBy("DATE").count().collect() )
    # # print("max data lines number: ", df_max_standard_all.groupBy("DATE").count().collect() )
    
    
    
    
    # df_result_maxdata_standard_2017 =  df_result_maxdata_standard.where( (df_result_maxdata_standard.DATE >=201701)& ((df_result_maxdata_standard.DATE < 201801) )   )
    # df_max_standard_all_2017 =  df_max_standard_all.where( (df_max_standard_all.DATE >=201701) & ((df_max_standard_all.DATE < 201801) )   ) \
    #                         .withColumnRenamed("SALES", "SALES_2" )
    # # for i in a:
    # #     print(i, df_result_maxdata_standard_2017.select(i).distinct().count(), df_result_maxdata_standard_2017.select(i).distinct().count() )
        
    # sales_error = df_result_maxdata_standard_2017.join( df_max_standard_all_2017, on=["DATE", "ID", "UNITS", "PHA", "MOLECULE", "PROVINCE","CITY", "MIN"],  how="inner")
    # # print( df_max_standard_all_2017.count(),  sales_error.count() )
    # sales_error.withColumn("Error", sales_error.SALES - sales_error.SALES_2 ).select("Error").show()  #.distinct().collect()
    
    
    # for i in [202001, 202002, 202003, 202004, 202005, 202006, 202007, 202008, 202009,202010, 202011, 202012 ]:
    #     df_result_maxdata_standard_2017 =  df_result_maxdata_standard.where( df_result_maxdata_standard.DATE == i )   
    #     df_max_standard_all_2017 =  df_max_standard_all.where(  df_max_standard_all.DATE ==i  ) \
    #                             .withColumnRenamed("Predict_Sales", "Predict_Sales_2" )
    #     # for i in a:
    #     #     print(i, df_result_maxdata_standard_2017.select(i).distinct().count(), df_result_maxdata_standard_2017.select(i).distinct().count() )
    
    #     sales_error = df_result_maxdata_standard_2017.join( df_max_standard_all_2017, on=['PROJECT', 'PROVINCE', 'CITY', 'DATE', 
    #                                                        'MIN_STD', 'MOLECULE', 'PANEL', 'MARKET',   
    #                                                         'MOLECULE_STD', 'BRAND_STD', 'FORM_STD', 'SPECIFICATIONS_STD', 
    #                                                         'PACK_NUMBER_STD', 'MANUFACTURER_STD', 'PROVINCE_STD', 
    #                                                         'CITY_STD',  'ATC', 'DATE_COPY'],
    #                                                        how="inner")
    #     coo =  sales_error.withColumn("Error", sales_error.Predict_Sales - sales_error.Predict_Sales_2 ).select("Error").count()
    #     print( df_max_standard_all_2017.count(),  sales_error.count(), coo, )
    
    #     sales_error = df_result_maxdata_standard_2017.join( df_max_standard_all_2017, on=['PROJECT', 'PROVINCE', 'CITY', 'DATE', 
    #                                                        'MIN_STD', 'MOLECULE', 'PANEL', 'MARKET',   
    #                                                         'MOLECULE_STD', 'BRAND_STD', 'FORM_STD', 'SPECIFICATIONS_STD', 
    #                                                         'PACK_NUMBER_STD', 'MANUFACTURER_STD', 'PROVINCE_STD', 
    #                                                         'CITY_STD',  'ATC', 'DATE_COPY'],
    #                                                        how="leftanti")
    #     sales_error_2 = df_max_standard_all_2017.join( df_result_maxdata_standard_2017, on=['PROJECT', 'PROVINCE', 'CITY', 'DATE', 
    #                                                        'MIN_STD', 'MOLECULE', 'PANEL', 'MARKET',   
    #                                                         'MOLECULE_STD', 'BRAND_STD', 'FORM_STD', 'SPECIFICATIONS_STD', 
    #                                                         'PACK_NUMBER_STD', 'MANUFACTURER_STD', 'PROVINCE_STD', 
    #                                                         'CITY_STD',  'ATC', 'DATE_COPY'],
    #                                                        how="leftanti")
    #     coo =  sales_error.withColumn("Error", sales_error.Predict_Sales - sales_error.Predict_Sales_2 ).select("Error").count()
    #     print(sales_error)
    #     sales_error.show(5 )
    #     sales_error_2.show(5)
    #     print(coo)
    #     print( df_max_standard_all_2017.count(),  sales_error.count(), coo, )
    #     break

