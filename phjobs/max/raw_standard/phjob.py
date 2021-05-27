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
    g_max_path = kwargs['g_max_path']
    g_base_path = kwargs['g_base_path']
    g_out_dir = kwargs['g_out_dir']
    g_year = kwargs['g_year']
    g_month = kwargs['g_month']
    g_monthly_update = kwargs['g_monthly_update']
    ### input args ###
    
    ### output args ###
    g_rawdata_standard = kwargs['g_rawdata_standard']
    g_rawdata_standard_brief = kwargs['g_rawdata_standard_brief']
    ### output args ###

    
    
    
    from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructType
    from pyspark.sql.functions import col
    from pyspark.sql import functions as func
    from pyspark.sql import DataFrame    
    # %%
    # ===========  测试用的参数 ============
    # dag_name = 'Max'
    # run_id = 'max_test_beida_202012'
    
    
    # dag_name = "Test_Max_model_1419"
    # run_id = "Test_Max_model_1419_Test_Max_model_1419_2021-05-26_16-38-25"
    # g_project_name ="贝达"
    # g_out_dir="202012_test"
    # result_path_prefix=get_result_path({"name":job_name, "dag_name":dag_name, "run_id":run_id})
    # depends_path=get_depends_path({"name":job_name, "dag_name":dag_name, 
    #                                  "run_id":run_id  })
    
    # g_monthly_update = 'False'
    # g_year = '2018'
    
    # # g_monthly_update = 'True'
    # # g_year = '2020'
    # # g_month = '12'
    # %%
    # =========== 输入 输出 ============
    
    g_year = int(g_year)
    if g_monthly_update == 'True':
        g_month = int(g_month)
    
    # 输出
    p_raw_data_standard = result_path_prefix + g_rawdata_standard
    p_raw_data_standard_brief = result_path_prefix +  g_rawdata_standard_brief

    # %%
    ### jupyter调试
    
    # p_raw_data_standard = p_raw_data_standard.replace("s3:","s3a:")
    # p_raw_data_standard_brief = p_raw_data_standard_brief.replace("s3:","s3a:")
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
    
    # 读取raw_data_delivery时需要用到
    createView(g_project_name, "raw_data_delivery_fact", "FACT/RAW_DATA_STD_DELIVERY_FACT", time = "2021-04-14")
    createView(g_project_name, "hospital_dimesion", "DIMENSION/HOSPITAL_DIMENSION", time = "2021-04-14")
    createView(g_project_name, "product_dimesion", "DIMENSION/PRODUCT_DIMENSION", time = "2021-04-14") 
    createView(g_project_name, "mnf_dimesion", "DIMENSION/MNF_DIMENSION", time = "2021-04-14")
    createView(g_project_name, "product_rel_dimesion", "DIMENSION/PRODUCT_RELATIONSHIP_DIMENSION", time = "2021-04-14")
    # 市场名匹配  mkt_mapping
    createView(g_project_name, "mole_market_mapping", "DIMENSION/MAPPING/MARKET_MOLE_MAPPING", time = "2021-04-14")
    
    # 匹配 Province, City，医院名:  base_universe
    ## hospital_dimesion, hospital_fact, hospital_base_seg_panel_mapping 
    createView(g_project_name, "hospital_fact", "FACT/HOSPITAL_FACT", time = "2021-04-14")
    createView(g_project_name, "hospital_base_seg_panel_mapping", "DIMENSION/MAPPING/HOSPITAL_BASE_MARKET_SEG_PANEL_MAPPING", 
               time = "2021-04-14")
    
    #省市标准化文件
    createView("PHARBERS", "city_normalize_mapping", "DIMENSION/MAPPING/CITY_NORMALIZE_MAPPING", time = "2021-04-14")        
    #packID- 标准通用名（MOLECULE_STD ）- ATC 匹配文件
    createView("PHARBERS", "universe_master_data", "DIMENSION/MAPPING/UNIVERSE_MASTER_DATA", time = "2021-04-14")
    #标准通用名（ MOLECULE_STD ） - ATC 匹配文件
    createView("PHARBERS", "product_map_all_atc", "DIMENSION/MAPPING/PRODUCT_MAPPING_ALL_ATC", time = "2021-04-14")
    #匹配PHA信息  cap_pha_mapping
    createView("PHARBERS", "fill_cpa_pha_mapping", "DIMENSION/MAPPING/CPA_GYC_MAPPING/BACKFILL", time = "2021-04-14")

    # %%
    # ===========  数据执行 ============
    
    # 一. 标准化匹配文件处理
    # 1. 城市标准化
    ## 读入城市标准化数据
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
    ## 读入 molecule-act 分子表
    product_map_all_atc_sql = """  SELECT * FROM product_map_all_atc """
    df_molecule_act_map = spark.sql(product_map_all_atc_sql)
    
    
    df_molecule_act_map = df_molecule_act_map.select(["MIN", "MAPPING_MAMOLE_NAME", "PACK_ID", "PROJECT", "MOLE_NAME", 
                                                      "ATC2_CODE", "ATC3_CODE", "ATC4_CODE"])
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
    
    raw_data_delivery_sql = """
            SELECT
                RW.RAWDATA_HOSP_NAME AS RAW_HOSP_NAME, RW.RAWDATA_MOLE_NAME AS MOLECULE, RW.RAWDATA_PRODUCT_NAME AS BRAND,
                RW.RAWDATA_DOSAGE AS FORM, RW.RAWDATA_SPEC AS SPECIFICATIONS, RW.RAWDATA_PACK AS PACK_NUMBER, RW.RAWDATA_MANUFACTURER AS MANUFACTURER,
                RW.RAW_CODE AS ID, HD.PANEL_ID AS PHA_ID, RW.RAW_PACK_ID AS PACK_ID, RW.RAW_MANUFACTURER AS MANUFACTURER_STD, RW.DATE,
                RW.RAW_MOLE_NAME AS MOLECULE_STD,  RW.RAW_PRODUCT_NAME AS BRAND_STD, 
                RW.RAW_PACK AS PACK_NUMBER_STD, RW.RAW_DOSAGE AS FORM_STD, RW.RAW_SPEC AS SPECIFICATIONS_STD,
                RW.SALES, RW.UNITS, RW.SOURCE
            FROM raw_data_delivery_fact AS RW
                LEFT JOIN hospital_dimesion AS HD ON RW.HOSPITAL_ID == HD.ID
                LEFT JOIN product_dimesion AS PD ON RW.PRODUCT_ID == PD.ID
                LEFT JOIN mnf_dimesion AS MD ON PD.MNF_ID == MD.ID
                LEFT JOIN product_rel_dimesion AS PRM ON PD.PACK_ID == PRM.ID AND PRM.CATEGORY = 'IMS PACKID'
        """
    
    df_raw_data_delivery = spark.sql(raw_data_delivery_sql)
    df_raw_data_delivery = df_raw_data_delivery.withColumn("DATE", col("DATE").cast("int"))\
                                                .withColumn("SALES", col("SALES").cast("double"))\
                                                .withColumn("UNITS", col("UNITS").cast("double"))
    
    ### 日期的确定
    if g_monthly_update == 'True':
        df_raw_data_delivery = df_raw_data_delivery.where(col('DATE') == (g_year*100 + g_month))
    elif g_monthly_update == 'False':
        df_raw_data_delivery = df_raw_data_delivery.where((col('DATE')/100).cast(IntegerType()) == g_year)
    
    df_raw_data_delivery.persist()

    # %%
    # 1. df_cpa_pha_mapping 匹配获得 PHA
    ## 读取 PHA数据
    fill_cpa_pha_mapping_sql = """   SELECT ID, PHA FROM fill_cpa_pha_mapping """
    df_cpa_pha_mapping = spark.sql(fill_cpa_pha_mapping_sql).distinct()

    # %%
    df_raw_data = df_raw_data_delivery.join( df_cpa_pha_mapping, on="ID", how="left")
    # %%
    # 2. universe PHA 匹配获得 HOSP_NAME，Province，City
    # 读取 univers数据
    p_universe = g_max_path + "/Common_files/universe_latest"
    df_universe_old = spark.read.parquet(p_universe)
    
    df_universe_old = df_universe_old.select('新版ID', '新版名称', 'Province', 'City').distinct() \
                        .withColumnRenamed('新版ID', 'PHA') \
                        .withColumnRenamed('新版名称', 'PHA_HOSPITAL_NAME')\
                        .withColumnRenamed('Province', 'PROVINCE')\
                        .withColumnRenamed('City', 'CITY')
    df_universe = df_universe_old
    # %%
    # 2. universe PHA 匹配获得 HOSP_NAME，Province，City
    # base_universe_sql = """
    #     SELECT  PHA, HOSPITAL_ID, HOSP_NAME,
    #             PROVINCE, CITY, CITY_TIER,
    #             REGION, TOTAL AS BEDSIZE, PANEL,
    #             SEG, MEDICINE_RMB AS EST_DRUGINCOME_RMB
    #     FROM (
    #         SELECT
    #             hfct.PANEL_ID AS PHA, hfct.HOSPITAL_ID,
    #             hdim.HOSP_NAME, hdim.PROVINCE, hdim.CITY, hdim.CITYGROUP AS CITY_TIER,
    #             hdim.REGION, hfct.TAG, hfct.VALUE
    #         FROM hospital_dimesion AS hdim
    #         INNER JOIN hospital_fact AS hfct ON hdim.ID == hfct.HOSPITAL_ID
    #         LEFT JOIN hospital_base_seg_panel_mapping AS hmsm ON hmsm.HOSPITAL_FACT_ID == hfct.ID
    #         WHERE
    #             (hfct.CATEGORY = 'BEDCAPACITY' AND hfct.TAG = 'TOTAL')
    #             OR
    #             (hfct.CATEGORY = 'REVENUE' AND hfct.TAG = 'MEDICINE_RMB')
    #             OR
    #             (hfct.CATEGORY = 'CPAGY' AND (hfct.TAG = 'SEG' OR hfct.TAG = 'PANEL'))
    #             AND
    #             (hmsm.CATEGORY = 'CPAGY' AND hmsm.TAG = 'BASE')
    #     )
    #     PIVOT (
    #         SUM(VALUE)
    #         FOR TAG in ('TOTAL', 'MEDICINE_RMB', 'SEG', 'PANEL')
    #     )
    # """
    # df_universe = spark.sql(base_universe_sql)
    # df_universe = df_universe.select(["PHA", "HOSP_NAME", "PROVINCE", "CITY" ])\
    #                                     .withColumnRenamed("HOSP_NAME", "PHA_HOSPITAL_NAME")
    # df_universe.persist()
    # df_universe.show(1,vertical=True)
    # %%
    ################################### 以下是比较 universe的结果时需要使用到
    # print(df_universe.count(), df_universe_old.count() )
    
    # df_universe_old.show(1, vertical=True)
    
    
    # df_universe_new = df_universe
    
    # print( df_universe_new.subtract(df_universe_old).count() )
    # print( df_universe_old.subtract(df_universe_new).count() )
    
    # # df_universe_new.where(df_universe_new.PHA=="PHA0030185").show()
    
    # df_universe_new.where(df_universe_new.PHA_HOSPITAL_NAME.contains("捷希")).show()
    # df_universe_old.subtract(df_universe_new).where(col("PHA")=="null" ).show()
    ###################################
    # %%
    ## raw-data 和 universe 连接
    df_raw_data_join_universe = df_raw_data.join(df_universe, on="PHA", how="left")
    
    
    ## 此处在原job基础上删除了 MIN
    #之前的 MIN是为了和product Map 进行匹配
    
    ## 此处对BRAMD列存在空的情况进行补全
    df_raw_data_join_universe = df_raw_data_join_universe.withColumn('BRAND', func.when((df_raw_data_join_universe.BRAND.isNull()) \
                                            | (df_raw_data_join_universe.BRAND == 'NA'), 
                                                df_raw_data_join_universe.MOLECULE).otherwise(df_raw_data_join_universe.BRAND))  

    # %%
    ## 读取市场表
    mole_market_mapping_sql = """  SELECT MARKET, COMMON_NAME AS MOLECULE_STD FROM mole_market_mapping """
    df_market_map = spark.sql(mole_market_mapping_sql)

    # %%
    # 三. 标准化df_raw_data_join_universe
    df_data_standard = df_raw_data_join_universe
    
    # 2. 匹配MOLECULE_STD: 获得 MARKET
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
    ## 胰岛素项目需要进行特殊处理
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
    df_data_standard = df_data_standard.withColumn("MOLECULE_STD_MASTER", func.when(df_data_standard['MOLECULE_STD_MASTER'].isNull(), 
                                                                                    df_data_standard['MOLECULE_STD']) \
                                                        .otherwise(df_data_standard['MOLECULE_STD_MASTER']))
    
    # city 标准化：
    '''
    先标准化省，再用(标准省份-City)标准化市
    '''
    df_data_standard = df_data_standard.join(df_max_city_normalize.select("PROVINCE", "PROVINCE_STD").distinct(), 
                                             on=["PROVINCE"], how="left")
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
    
    # print(df_data_standard.columns )
    
    std_names_list = ["DATE", "ID", "RAW_HOSP_NAME", 
                "MOLECULE", "BRAND", "FORM", "SPECIFICATIONS", "PACK_NUMBER", "MANUFACTURER", 
                "SALES", "UNITS", "UNITS_BOX", "PHA", 
                "PROVINCE", "CITY", "PROVINCE_STD", "CITY_STD", 
                "MARKET",  'PHA_HOSPITAL_NAME', 'SOURCE', 
                "MOLECULE_STD_MASTER", "BRAND_STD", "FORM_STD", "SPECIFICATIONS_STD", 
                "PACK_NUMBER_STD", "MANUFACTURER_STD", "PACK_ID", "ATC", "PROJECT" ]
    # 'CITY_TIER', 'REGION','BEDSIZE', 'PANEL',
    df_raw_data_standard = df_data_standard.select( std_names_list ).withColumnRenamed('MOLECULE_STD_MASTER', 'MOLECULE_STD')
    
    df_raw_data_standard = df_raw_data_standard.withColumn("DATE_COPY", df_raw_data_standard.DATE)
    
    # 目录结果汇总
    df_raw_data_standard_brief = df_raw_data_standard.select("PROJECT", "DATE", "MOLECULE_STD", "ATC", "MARKET", "PHA").distinct()

    # %%
    # =========== 数据输出 =============
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
    # p_result_rawdata_standard ="s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/test/贝达_rawdata_standard/"
    # df_result_rawdata_standard = spark.read.parquet( p_result_rawdata_standard )
    # # df_result_rawdata_standard.show(1, vertical=True)
    # # df_result_rawdata_standard = df_result_rawdata_standard.where(df_result_rawdata_standard.Date==202012)
    # df_result_rawdata_standard = df_result_rawdata_standard.where( (df_result_rawdata_standard.Date>=201801 ) &
    #                                                               (df_result_rawdata_standard.Date<=201812) )
    # # df_raw_data_standard.select("Date").distinct().show()
    
    # a = ['DATE', 'ID',  'BRAND', 'FORM', 'SPECIFICATIONS', 'PACK_NUMBER', 'MANUFACTURER', 'MOLECULE', 'SALES', 'UNITS', 'UNITS_BOX', 'PHA', 'RAW_HOSP_NAME', 'PROVINCE', 'CITY', 'MIN', 'MARKET', 'MOLECULE_STD', 'BRAND_STD', 'FORM_STD', 'SPECIFICATIONS_STD', 'PACK_NUMBER_STD', 'MANUFACTURER_STD', 'PROVINCE_STD', 'CITY_STD', 'PACK_ID', 'ATC', 'PROJECT', 'DATE_COPY']
    # b = ['Date', 'ID',  'Brand', 'Form', 'Specifications', 'Pack_Number', 'Manufacturer', 'Molecule', 'Sales', 'Units', 'Units_Box', 'PHA', 'PHA医院名称', 'Province', 'City', 'min1', 'DOI', '标准通用名', '标准商品名', '标准剂型', '标准规格', '标准包装数量', '标准生产企业', '标准省份名称', '标准城市名称', 'PACK_ID', 'ATC', 'project', 'Date_copy']
    # df_result_rawdata_standard =  df_result_rawdata_standard.select([col(i).alias(j)   for i,j in zip(b,a)])
    
    # old = df_result_rawdata_standard
    # new = df_raw_data_standard
    # old.agg(func.sum("SALES"), func.sum("UNITS")).show()
    # new.agg(func.sum("SALES"), func.sum("UNITS")).show()
    # %%
    # print(df_raw_data_standard.count(), df_result_rawdata_standard.count())
    
    # # com = list(set(df_result_rawdata_standard.columns)& set(df_raw_data_standard.columns) )
    # com =['ID', 'UNITS', 'BRAND', 'PHA', 'ATC',  'MARKET', 'CITY_STD', 'FORM_STD', 'PROVINCE_STD', 'SPECIFICATIONS_STD', 
    #       'PACK_NUMBER_STD', 'MOLECULE_STD', 'PACK_ID', 'BRAND_STD', 
    #       'DATE', 'PROVINCE', 'SALES', 'MANUFACTURER_STD', 'DATE_COPY', 'PROJECT', 'CITY']
    # old = df_result_rawdata_standard.select(com)
    # new = df_raw_data_standard.select(com).withColumn("DATE_COPY", col("DATE_COPY").cast("int"))
    
    # print(new.subtract(old).count(),  old.subtract(new).count())
    
    # old.subtract(new).show(1, vertical=True)
    # new.subtract(old).show()
    
    # new.show(1, vertical=True)
    # old.show(1, vertical=True)
    
    # old.subtract(new).show()
    # new.subtract(old).show()
    # %%
    
    # df_result_rawdata_standard_2017 =  df_result_rawdata_standard.where( (df_result_rawdata_standard.DATE >=201701)& ((df_result_rawdata_standard.DATE < 201801) )   )
    # df_raw_data_standard_2017 =  df_raw_data_standard.where( (df_raw_data_standard.DATE >=201701) & ((df_raw_data_standard.DATE < 201801) )   ) \
    #   .withColumnRenamed("SALES", "SALES_2" )
    
    # # 比较每个月的样本数目是否一致
    # # for i in a:
    # #     print(i, df_result_rawdata_standard_2017.select(i).distinct().count(), df_result_rawdata_standard_2017.select(i).distinct().count() )
        
    # # 比较单独一年，新旧 结果是否一致    
    # sales_error = df_result_rawdata_standard_2017.join( df_raw_data_standard_2017, on=["DATE", "ID", "UNITS", "PHA", "MOLECULE", "PROVINCE","CITY" ],  how="inner")
    # # print( df_raw_data_standard_2017.count(),  sales_error.count() )
    # sales_error.withColumn("Error", sales_error.SALES - sales_error.SALES_2 ).select("Error").show()  #.distinct().collect()
    
    
    ## 循环比较每个年，新旧结果是否一致
    # for i in [201701,201801, 201901, 202001 ]:
    #     df_result_rawdata_standard_2017 =  df_result_rawdata_standard.where( (df_result_rawdata_standard.DATE >=i )& ((df_result_rawdata_standard.DATE < (i+100) ) )   )
    #     df_raw_data_standard_2017 =  df_raw_data_standard.where( (df_raw_data_standard.DATE >=i ) & ((df_raw_data_standard.DATE < (i+100)  ) )   ) \
    #                             .withColumnRenamed("SALES", "SALES_2" )
        
    #     sales_error = df_result_rawdata_standard_2017.join( df_raw_data_standard_2017, on=["DATE", "ID", "UNITS", "PHA", "MOLECULE", "PROVINCE","CITY"],  how="inner")
    #     print( "Old-lines: %s   New-lines: %s   inner-lines: %s"% ( df_result_rawdata_standard_2017.count(), 
    #           df_raw_data_standard_2017.count(),  sales_error.count() ) )
    #     coo =  sales_error.withColumn("Error", sales_error.SALES - sales_error.SALES_2 ).select("Error").distinct().collect()
    #     print(coo)

