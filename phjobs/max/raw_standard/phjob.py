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
    g_path_max = kwargs['g_path_max']
    g_path_extract = kwargs['g_path_extract']
    g_project_name = kwargs['g_project_name']
    g_out_dir = kwargs['g_out_dir']
    g_minimum_product_sep = kwargs['g_minimum_product_sep']
    g_minimum_product_columns = kwargs['g_minimum_product_columns']
    g_dag_name = kwargs['g_dag_name']
    g_run_id = kwargs['g_run_id']
    g_depend_job_names_keys = kwargs['g_depend_job_names_keys']
    ### input args ###
    
    ### output args ###
    g_rawdata_standard = kwargs['g_rawdata_standard']
    g_rawdata_standard_brief = kwargs['g_rawdata_standard_brief']
    ### output args ###

    # ===========  加载需要使用到的包 ============
    from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructType
    from pyspark.sql.functions import col
    from pyspark.sql import functions as func
    from pyspark.sql import DataFrame
    # %%
    # ===========  测试用的参数 ============
    # g_project_name ="贝达"
    # g_out_dir = "202012_test"
    # %%
    # ===========  输入文件和输出文件目录 ============
    
    if g_minimum_product_sep == 'kong':
        g_minimum_product_sep = ''
    g_minimum_product_columns = g_minimum_product_columns.replace(' ', '').split(',')
    
    
    # raw—data数据，product-map产品匹配数据，市场数据
    p_raw_data = g_path_max + '/' + g_project_name + '/' + g_out_dir + '/raw_data_delivery'
    p_product_map = g_path_max + "/" + g_project_name + "/" + g_out_dir + "/prod_mapping"
    p_market  = g_path_max + "/" + g_project_name + "/mkt_mapping"
    
    # 通用匹配文件
    p_cpa_pha_mapping = g_path_max + "/Common_files/cpa_pha_mapping"
    p_universe = g_path_max + "/Common_files/universe_latest"
    p_molecule_act = g_path_max  + "/Common_files/extract_data_files/product_map_all_ATC.csv"
    p_max_city_normalize = g_path_max  + "/Common_files/extract_data_files/MAX_city_normalize.csv"
    p_master_data_map = g_path_max  + "/Common_files/extract_data_files/master_data_map.csv"
    
    # 输出
    p_raw_data_standard = result_path_prefix + g_rawdata_standard
    p_raw_data_standard_brief = result_path_prefix +  g_rawdata_standard_brief
    
    # print(p_raw_data_standard)
    # print(p_raw_data_standard_brief)
    # %%
    # ===========  数据执行 ============
    
    # 一. 标准化匹配文件处理
    # 1. 城市标准化
    ## 读入城市标准化数据
    df_max_city_normalize = spark.read.csv(p_max_city_normalize, header=True)
    df_max_city_normalize = df_max_city_normalize.withColumnRenamed("Province", "PROVINCE")\
                                                .withColumnRenamed("City", "CITY")\
                                                .withColumnRenamed("标准省份名称", "PROVINCE_STD")\
                                                .withColumnRenamed("标准城市名称", "CITY_STD")
    
    # 2. master_data_map：PACK_ID - MOLECULE_STD_MASTER - ACT, 无缺失
    ## 读入 master—data-map
    df_master_data_map = spark.read.csv(p_master_data_map, header=True)
    
    df_pack_id_master_map = df_master_data_map.select("PACK_ID", "MOLE_NAME_CH", "PROD_NAME_CH", "CORP_NAME_CH", 
                                                      "DOSAGE", "SPEC", "PACK", "ATC4_CODE") \
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
    # 3. product_map_all_ATC: 有补充的新的 PACK_ID - 标准通用名 - ACT （0是缺失）
    ## 读入 molecule-act 分子表
    df_molecule_act_map = spark.read.csv(p_molecule_act, header=True)
    
    df_molecule_act_map = df_molecule_act_map.withColumnRenamed("PackID", "PACK_ID" )\
                                            .withColumnRenamed("min2", "MIN_STD") \
                                            .withColumnRenamed("通用名", "MOLECULE_STD") \
                                            .withColumnRenamed("pfc", "PFC")  \
                                            .withColumnRenamed("project", "PROJECT") \
                                            .withColumnRenamed("标准商品用名", "BRAND_STD") \
                                            .withColumnRenamed("标准剂型", "FORM_STD")\
                                            .withColumnRenamed("标准规格", "SPECIFICATIONS_STD") \
                                            .withColumnRenamed("标准包装数量", "PACK_NUMBER_STD") \
                                            .withColumnRenamed("标准生产企业", "MANUFACTURER_STD") \
                                            .withColumnRenamed("PackID", "PACK_ID")                                        
    
    
    df_add_pack_id = df_molecule_act_map.where(df_molecule_act_map.PROJECT == g_project_name) \
                    .select("MIN_STD", "PACK_ID").distinct() \
                    .withColumn("PACK_ID", df_molecule_act_map.PACK_ID.cast(IntegerType()))
    df_add_pack_id = df_add_pack_id.withColumn("PACK_ID", 
                    func.when(df_add_pack_id.PACK_ID == "0", None) \
                        .otherwise(df_add_pack_id.PACK_ID)  )  \
                    .withColumnRenamed("PACK_ID", "PACK_ID_ADD") 
    
    df_molecule_act_map = df_molecule_act_map.select("MOLECULE_STD", "MOLE_NAME_CH", "ATC4_CODE") \
                    .withColumnRenamed("MOLE_NAME_CH", "MOLE_NAME_CH_2") \
                    .withColumnRenamed("ATC4_CODE", "ATC4_2") \
                    .dropDuplicates(["MOLECULE_STD"])
    df_molecule_act_map = df_molecule_act_map.withColumn("MOLE_NAME_CH_2", 
                                                         func.when(df_molecule_act_map.MOLE_NAME_CH_2 == "0", None) \
                                                         .otherwise(df_molecule_act_map.MOLE_NAME_CH_2)) \
                                            .withColumn("ATC4_2", 
                                                        func.when(df_molecule_act_map.ATC4_2 == "0", None)\
                                                        .otherwise(df_molecule_act_map.ATC4_2))

    # %%
    # 4. 产品信息，列名标准化
    ## 读入产品表
    
    df_product_map = spark.read.parquet(p_product_map)
    df_product_map = df_product_map.withColumnRenamed('min1', 'MIN') \
                        .withColumnRenamed('min2', 'MIN_STD') \
                        .withColumnRenamed('标准通用名', 'MOLECULE_STD') \
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
                    .select("MIN", "MIN_STD", "PACK_ID", "MOLECULE_STD", "BRAND_STD", "FORM_STD", 
                            "SPECIFICATIONS_STD", "PACK_NUMBER_STD", "MANUFACTURER_STD") \
                    .withColumn("PACK_ID", df_product_map["PACK_ID"].cast(IntegerType())) \
                    .withColumn("PACK_NUMBER_STD", df_product_map["PACK_NUMBER_STD"].cast(IntegerType())) \
                    .distinct()
    
    
    # c. PACK_ID为0统一替换为null
    df_product_map = df_product_map.withColumn("PACK_ID", func.when(df_product_map.PACK_ID == 0, None) \
                                               .otherwise(df_product_map.PACK_ID)).distinct()
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
    df_product_map = df_product_map.dropDuplicates(["MIN"])

    # %%
    # 二. raw_data 基础信息匹配
    '''
    raw_data 用 MIN 匹配 df_product_map的标准列
    '''
    ## 读取raw-data数据
    df_raw_data = spark.read.parquet(p_raw_data)
    df_raw_data = df_raw_data.select([col(i).alias( i.upper() ) for i in df_raw_data.columns])
    
    
    for colname, coltype in df_raw_data.dtypes:
        if coltype == "logical":
            df_raw_data = df_raw_data.withColumn(colname, df_raw_data[colname].cast(StringType()))
            
    # 1. df_cpa_pha_mapping 匹配获得 PHA
    ## 读取 PHA数据
    df_cpa_pha_mapping = spark.read.parquet(p_cpa_pha_mapping)
    df_cpa_pha_mapping = df_cpa_pha_mapping.withColumnRenamed("推荐版本", "COMMEND")
    df_cpa_pha_mapping = df_cpa_pha_mapping.where(df_cpa_pha_mapping["COMMEND"] == 1) \
        .select("ID", "PHA").distinct()
    
    
    
    def dealIdLength(df:DataFrame)->DataFrame:
        # ID 的长度统一
        def distinguishCpaGyc(col, gyc_hospital_id_length):
            # gyc_hospital_id_length是国药诚信医院编码长度，一般是7位数字，cpa医院编码一般是6位数字。医院编码长度可以用来区分cpa和gyc
            return (func.length(col) < gyc_hospital_id_length)
        
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(distinguishCpaGyc(df.ID, 7), 
                                    func.lpad(df.ID, 6, "0")).otherwise(df.ID) )
        return df
    
    df_cpa_pha_mapping = dealIdLength(df_cpa_pha_mapping)
    df_raw_data = dealIdLength(df_raw_data)                                
    df_raw_data = df_raw_data.join(df_cpa_pha_mapping, on="ID", how="left")
    
    

    # %%
    # 2. universe PHA 匹配获得 HOSP_NAME，Province，City
    # 读取 univers数据
    df_universe = spark.read.parquet(p_universe)
    
    df_universe = df_universe.select('新版ID', '新版名称', 'Province', 'City').distinct() \
                        .withColumnRenamed('新版ID', 'PHA') \
                        .withColumnRenamed('新版名称', 'PHA_HOSPITAL_NAME')\
                        .withColumnRenamed('Province', 'PROVINCE')\
                        .withColumnRenamed('City', 'CITY')
    
    
    df_raw_data_join_universe = df_raw_data.join(df_universe, on="PHA", how="left")
    
    
    # # 3. 生成 MIN
    # Mylan不重新生成 MIN，其他项目生成 MIN
    if g_project_name != "Mylan":
        if "MIN" in df_raw_data_join_universe.columns:
            df_raw_data_join_universe = df_raw_data_join_universe.drop("MIN")
        df_raw_data_join_universe = df_raw_data_join_universe.withColumn('BRAND', func.when((df_raw_data_join_universe.BRAND.isNull()) \
                                                | (df_raw_data_join_universe.BRAND == 'NA'), 
                                                    df_raw_data_join_universe.MOLECULE).otherwise(df_raw_data_join_universe.BRAND))  
    
        df_raw_data_join_universe = df_raw_data_join_universe.withColumn('MIN', func.concat_ws(g_minimum_product_sep, 
                                        *[func.when(col(i).isNull(), func.lit("NA")).otherwise(col(i)) for i in g_minimum_product_columns]))
    # %%
    # 三. 标准化df_raw_data_join_universe
    # 2. df_product_map 匹配 MIN ：获得 PACK_ID, MOLECULE_STD, 标准商品名, 标准剂型, 标准规格, 标准包装数量, 标准生产企业
    if g_project_name == "Mylan":
        df_product_map = df_product_map.drop('PACK_ID')
        df_raw_data_join_universe = df_raw_data_join_universe.withColumnRenamed('Pack_ID', 'PACK_ID')
    df_data_standard = df_raw_data_join_universe.join(df_product_map, on='MIN', how="left")
    
    
    
    # 市场名匹配
    df_market_map = spark.read.parquet(p_market)
    df_market_map = df_market_map.withColumnRenamed("标准通用名", "MOLECULE_STD") \
        .withColumnRenamed("model", "MARKET") \
        .select("MARKET", "MOLECULE_STD").distinct()
    
    
    
    df_data_standard = df_data_standard.join(df_market_map, on='MOLECULE_STD', how='left')
    
    # 3. df_pack_id_master_map 匹配 PACK_ID ：获得master标准列 --- MOLE_NAME_CH_1, ATC4_1, PROD_NAME_CH, "CORP_NAME_CH, DOSAGE, SPEC, PACK
    df_data_standard = df_data_standard.join(df_pack_id_master_map, on=["PACK_ID"], how="left")
    
    # 4. df_molecule_act_map 匹配 MOLECULE_STD：获得master标准分子名和ATC -- MOLE_NAME_CH_2, ATC4_2
    df_data_standard = df_data_standard.join(df_molecule_act_map, on=["MOLECULE_STD"], how="left")
    
    # 5. 整合 master 匹配结果 和 df_product_map, df_molecule_act_map 匹配结果
    '''
    ATC4_1 和 MOLE_NAME_CH_1 来自 master 有 pack_id 匹配得到 ; ATC4_2 和 MOLE_NAME_CH_2 来自 df_molecule_act_map 
    '''
    # A10C/D/E是胰岛素, MOLECULE_STD和公司名用master, 其他信息用df_product_map
    df_data_standard = df_data_standard.withColumn("ATC", func.when(df_data_standard["ATC4_1"].isNull(), df_data_standard["ATC4_2"]) \
                                            .otherwise(df_data_standard["ATC4_1"]))

    # %%
    df_data_standard_yidaosu = df_data_standard.where(func.substring(df_data_standard.ATC, 0, 4).isin(['A10C', 'A10D', 'A10E'])) \
                            .withColumn("PROD_NAME_CH", df_data_standard['BRAND_STD']) \
                            .withColumn("DOSAGE", df_data_standard['FORM_STD']) \
                            .withColumn("SPEC", df_data_standard['SPECIFICATIONS_STD']) \
                            .withColumn("PACK", df_data_standard['PACK_NUMBER_STD'])
    
    
    df_data_standard_others = df_data_standard.where((~func.substring( 
                    df_data_standard.ATC, 0, 4).isin(['A10C', 'A10D', 'A10E'])) | df_data_standard.ATC.isNull())
    
    # # 合并 max_standard_yidaosu 和 max_standard_others
    df_data_standard = df_data_standard_others.union(df_data_standard_yidaosu.select(df_data_standard_others.columns))
    # %%
    # # master 匹配不上的(ATC4_1是null) 用 df_molecule_act_map 和 df_product_map 信息
    df_data_standard = df_data_standard.withColumn("MOLECULE_STD_MASTER", func.when(df_data_standard["MOLE_NAME_CH_1"].isNull(), df_data_standard["MOLE_NAME_CH_2"]) \
                                            .otherwise(df_data_standard["MOLE_NAME_CH_1"])) \
                            .withColumn("BRAND_STD", func.when(df_data_standard["ATC4_1"].isNull(), df_data_standard["BRAND_STD"]) \
                                            .otherwise(df_data_standard["PROD_NAME_CH"])) \
                            .withColumn("FORM_STD", func.when(df_data_standard["ATC4_1"].isNull(), df_data_standard["FORM_STD"]) \
                                            .otherwise(df_data_standard["DOSAGE"])) \
                            .withColumn("SPECIFICATIONS_STD", func.when(df_data_standard["ATC4_1"].isNull(), df_data_standard["SPECIFICATIONS_STD"]) \
                                            .otherwise(df_data_standard["SPEC"])) \
                            .withColumn("PACK_NUMBER_STD", func.when(df_data_standard["ATC4_1"].isNull(), df_data_standard["PACK_NUMBER_STD"]) \
                                            .otherwise(df_data_standard["PACK"])) \
                            .withColumn("MANUFACTURER_STD", func.when(df_data_standard["ATC4_1"].isNull(), df_data_standard["MANUFACTURER_STD"]) \
                                            .otherwise(df_data_standard["CORP_NAME_CH"])) \
                            .drop("ATC4_1", "ATC4_2", "MOLE_NAME_CH_1", "MOLE_NAME_CH_2")
    
    # # MOLE_NAME_CH_1 和 MOLE_NAME_CH_2 没有标准通用名的 用原始的通用名
    df_data_standard = df_data_standard.withColumn("MOLECULE_STD_MASTER", func.when(df_data_standard['MOLECULE_STD_MASTER'].isNull(), df_data_standard['MOLECULE_STD']) \
                                                        .otherwise(df_data_standard['MOLECULE_STD_MASTER']))
    
    # city 标准化：
    '''
    先标准化省，再用(标准省份-City)标准化市
    '''
    df_data_standard = df_data_standard.join(df_max_city_normalize.select("PROVINCE", "PROVINCE_STD").distinct(), on=["PROVINCE"], how="left")
    df_data_standard = df_data_standard.join(df_max_city_normalize.select("CITY", "PROVINCE_STD", "CITY_STD").distinct(),
                            on=["PROVINCE_STD", "CITY"], how="left")
    
    ############################################## 测试数据时,需要如下的判断
    if "RAW_HOSP_NAME" not in df_data_standard.columns:
        df_data_standard = df_data_standard.withColumn("RAW_HOSP_NAME", func.lit("0"))
    if "UNITS_BOX" not in df_data_standard.columns:
        df_data_standard = df_data_standard.withColumn("UNITS_BOX", func.lit(0))
        
    # 有的项目RAW_HOSP_NAME全都为null，会在提数中间结果写出再读取时引起报错
    df_data_standard = df_data_standard.withColumn("RAW_HOSP_NAME", func.when(df_data_standard.RAW_HOSP_NAME.isNull(), func.lit("0")) \
                                                                    .otherwise(df_data_standard.RAW_HOSP_NAME))
    
    for each in df_data_standard.columns:                                                                
        df_data_standard = df_data_standard.withColumn(each, df_data_standard[each].cast(StringType()))
    
        
    df_data_standard = df_data_standard.withColumn("PROJECT", func.lit(g_project_name))
    
    
    
    std_names_list = ["DATE", "ID", "RAW_HOSP_NAME", "BRAND", "FORM", 
                "Specifications", "Pack_Number", "Manufacturer", "MOLECULE",
                "SOURCE", "SALES", "UNITS", "UNITS_BOX", "PHA", 
                "PHA_HOSPITAL_NAME", "PROVINCE", "CITY", "MIN", 
                "MARKET", "MOLECULE_STD_MASTER", "BRAND_STD", "FORM_STD", "SPECIFICATIONS_STD", 
                "PACK_NUMBER_STD", "MANUFACTURER_STD", "PROVINCE_STD", 
                "CITY_STD", "PACK_ID", "ATC", "PROJECT" ]
    df_raw_data_standard = df_data_standard.select( std_names_list ).withColumnRenamed("Specifications", "SPECIFICATIONS" )\
                .withColumnRenamed("Pack_Number", "PACK_NUMBER") \
                .withColumnRenamed("Manufacturer", "MANUFACTURER" )
    
    df_raw_data_standard = df_raw_data_standard.withColumn("DATE_COPY", df_raw_data_standard.DATE)
    
    df_raw_data_standard = df_raw_data_standard.withColumnRenamed('MOLECULE_STD_MASTER', 'MOLECULE_STD')

    # %%
    # ===========  保存结果,写入到文件中  ============
    # 目录结果汇总
    df_raw_data_standard_brief = df_raw_data_standard.select("PROJECT", "DATE", "MOLECULE_STD", "ATC", "MARKET", "PHA", "SOURCE").distinct()
    
    # 根据日期分桶写出
    df_raw_data_standard = df_raw_data_standard.repartition("DATE_COPY")
    df_raw_data_standard.write.format("parquet").partitionBy("DATE_COPY") \
        .mode("overwrite").save(p_raw_data_standard)
    
    # 输出brief结果
    df_raw_data_standard_brief = df_raw_data_standard_brief.repartition(2)
    df_raw_data_standard_brief.write.format("parquet") \
        .mode("overwrite").save(p_raw_data_standard_brief)
    # %%
    # ===========  数据校准  ============
    # p_result_rawdata_standard = "s3a://ph-stream/common/public/max_result/0.0.5/rawdata_standard/贝达_rawdata_standard"
    # df_result_rawdata_standard = spark.read.parquet( p_result_rawdata_standard )
    # df_result_rawdata_standard.persist()
    
    
    
    # print( df_raw_data_standard.columns)
    # print( df_result_rawdata_standard.columns )
    
    # a = ['DATE', 'ID', 'RAW_HOSP_NAME', 'BRAND', 'FORM', 'SPECIFICATIONS', 'PACK_NUMBER', 'MANUFACTURER', 'MOLECULE', 'SOURCE', 'SALES', 'UNITS', 'UNITS_BOX', 'PHA', 'PHA_HOSPITAL_NAME', 'PROVINCE', 'CITY', 'MIN', 'MARKET', 'MOLECULE_STD_MASTER', 'BRAND_STD', 'FORM_STD', 'SPECIFICATIONS_STD', 'PACK_NUMBER_STD', 'MANUFACTURER_STD', 'PROVINCE_STD', 'CITY_STD', 'PACK_ID', 'ATC', 'PROJECT', 'DATE_COPY']
    # b = ['Date', 'ID', 'Raw_Hosp_Name', 'Brand', 'Form', 'Specifications', 'Pack_Number', 'Manufacturer', 'Molecule', 'Source', 'Sales', 'Units', 'Units_Box', 'PHA', 'PHA医院名称', 'Province', 'City', 'min1', 'DOI', '标准通用名', '标准商品名', '标准剂型', '标准规格', '标准包装数量', '标准生产企业', '标准省份名称', '标准城市名称', 'PACK_ID', 'ATC', 'project', 'Date_copy']
    # df_result_rawdata_standard =  df_result_rawdata_standard.select([col(i).alias(j)   for i,j in zip(b,a)])
    # print( df_result_rawdata_standard.columns )
    
    
    ## 列长度一样
    # len(df_result_rawdata_standard.columns) == len( df_raw_data_standard.columns )
    # df_result_rawdata_standard.show(1, vertical=True)
    
    # 日期数目不同
    # df_result_rawdata_standard: 49个月
    # df_raw_data_standard: 48 个月
    # df_result_rawdata_standard.select("Date").distinct().show()
    # df_raw_data_standard.select("DATE").distinct().show()
    # print( df_result_rawdata_standard.select("Date").distinct().count(), df_raw_data_standard.select("DATE").distinct().count() )
    
    # df_raw_data_standard.show(1, vertical=True)
    ### 月份数目不同，所以总的数据量不一致
    # print("result   lines number: ", df_result_rawdata_standard.count() )
    # print("raw data lines number: ", df_raw_data_standard.count() )
    ### 限定月份数目
    # print("result   lines number: ", df_result_rawdata_standard.where( (df_result_rawdata_standard.Date >=202001)& ((df_result_rawdata_standard.Date < 202101) )   ).count() )
    # print("raw data lines number: ", df_raw_data_standard.where( (df_raw_data_standard.DATE >=202001) & ((df_raw_data_standard.DATE < 202101) )   ).count() )
    # print("result   lines number: ", df_result_rawdata_standard.where( (df_result_rawdata_standard.Date >=201901)& ((df_result_rawdata_standard.Date < 202001) )   ).count() )
    # print("raw data lines number: ", df_raw_data_standard.where( (df_raw_data_standard.DATE >=201901) & ((df_raw_data_standard.DATE < 202001) )   ).count() )
    # print("result   lines number: ", df_result_rawdata_standard.where( (df_result_rawdata_standard.Date >=201801)& ((df_result_rawdata_standard.Date < 201901) )   ).count() )
    # print("raw data lines number: ", df_raw_data_standard.where( (df_raw_data_standard.DATE >=201801) & ((df_raw_data_standard.DATE < 201901) )   ).count() )
    # print("result   lines number: ", df_result_rawdata_standard.where( (df_result_rawdata_standard.Date >=201701)& ((df_result_rawdata_standard.Date < 201801) )   ).count() )
    # print("raw data lines number: ", df_raw_data_standard.where( (df_raw_data_standard.DATE >=201701) & ((df_raw_data_standard.DATE < 201801) )   ).count() )
    
    
    # df_result_rawdata_standard.select("Date").distinct().show()
    # df_result_rawdata_standard.select("Date").where( df_result_rawdata_standard.Date >=202101  ).distinct().show()
    # df_result_rawdata_standard.select("Date").where( (df_result_rawdata_standard.Date >=202001)| ((df_result_rawdata_standard.Date >=202101) )   ).distinct().show()
    # 17，18，19，20，21(只有1月的)
    
    
    # df_result_rawdata_standard_2017 =  df_result_rawdata_standard.where( (df_result_rawdata_standard.DATE >=201701)& ((df_result_rawdata_standard.DATE < 201801) )   )
    # df_raw_data_standard_2017 =  df_raw_data_standard.where( (df_raw_data_standard.DATE >=201701) & ((df_raw_data_standard.DATE < 201801) )   ) \
    #                         .withColumnRenamed("SALES", "SALES_2" )
    # # for i in a:
    # #     print(i, df_result_rawdata_standard_2017.select(i).distinct().count(), df_result_rawdata_standard_2017.select(i).distinct().count() )
        
    # sales_error = df_result_rawdata_standard_2017.join( df_raw_data_standard_2017, on=["DATE", "ID", "UNITS", "PHA", "MOLECULE", "PROVINCE","CITY", "MIN"],  how="inner")
    # # print( df_raw_data_standard_2017.count(),  sales_error.count() )
    # sales_error.withColumn("Error", sales_error.SALES - sales_error.SALES_2 ).select("Error").show()  #.distinct().collect()
    
    
    # for i in [201701,201801, 201901, 202001 ]:
    #     df_result_rawdata_standard_2017 =  df_result_rawdata_standard.where( (df_result_rawdata_standard.DATE >=i )& ((df_result_rawdata_standard.DATE < (i+100) ) )   )
    #     df_raw_data_standard_2017 =  df_raw_data_standard.where( (df_raw_data_standard.DATE >=i ) & ((df_raw_data_standard.DATE < (i+100)  ) )   ) \
    #                             .withColumnRenamed("SALES", "SALES_2" )
    #     # for i in a:
    #     #     print(i, df_result_rawdata_standard_2017.select(i).distinct().count(), df_result_rawdata_standard_2017.select(i).distinct().count() )
    
    #     sales_error = df_result_rawdata_standard_2017.join( df_raw_data_standard_2017, on=["DATE", "ID", "UNITS", "PHA", "MOLECULE", "PROVINCE","CITY", "MIN"],  how="inner")
    #     # print( df_raw_data_standard_2017.count(),  sales_error.count() )
    #     coo =  sales_error.withColumn("Error", sales_error.SALES - sales_error.SALES_2 ).select("Error").distinct().collect()
    #     print(coo)

