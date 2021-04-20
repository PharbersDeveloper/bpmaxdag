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
    g_minimum_product_columns = kwargs['g_minimum_product_columns']
    g_minimum_product_sep = kwargs['g_minimum_product_sep']
    g_minimum_product_newname = kwargs['g_minimum_product_newname']
    g_if_two_source = kwargs['g_if_two_source']
    g_hospital_level = kwargs['g_hospital_level']
    g_bedsize = kwargs['g_bedsize']
    p_id_bedsize = kwargs['p_id_bedsize']
    g_time_left = kwargs['g_time_left']
    g_time_right = kwargs['g_time_right']
    g_market = kwargs['g_market']
    depend_job_names_keys = kwargs['depend_job_names_keys']
    g_max_path = kwargs['g_max_path']
    g_out_dir = kwargs['g_out_dir']
    dag_name = kwargs['dag_name']
    run_id = kwargs['run_id']
    ### input args ###
    
    ### output args ###
    g_max_result_city = kwargs['g_max_result_city']
    ### output args ###

    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType,StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import col
    import boto3
    import os        # %%
    '''
    g_project_name = "贝达"
    g_market = 'BD1'
    g_time_left = "202012"
    g_time_right = "202012"
    g_out_dir = "202012_test"
    g_if_two_source = "True"
                                     "run_id":run_id, "depend_job_names_keys":depend_job_names_keys})
    '''

    # %%
    # 输入
    g_minimum_product_columns = g_minimum_product_columns.replace(" ","").split(",")
    if g_minimum_product_sep == "kong":
        g_minimum_product_sep = ""
    
    if g_bedsize != "False" and g_bedsize != "True":
        raise ValueError('g_bedsize: False or True')
    if g_hospital_level != "False" and g_hospital_level != "True":
        raise ValueError('g_hospital_level: False or True')
    
    g_time_left = int(g_time_left)
    g_time_right = int(g_time_right)
    
    p_province_city_mapping = g_max_path + "/" + g_project_name + '/province_city_mapping'
    p_market = g_max_path + "/" + g_project_name + '/mkt_mapping'
    p_cpa_pha_mapping = g_max_path + "/" + g_project_name + "/cpa_pha_mapping"
    if p_id_bedsize == 'Empty':
        p_id_bedsize = g_max_path + "/Common_files/ID_Bedsize"
    else:
        p_id_bedsize = p_id_bedsize
    
    p_cpa_pha_mapping_common = g_max_path + "/Common_files/cpa_pha_mapping"
    
    out_path_dir = g_max_path + "/" + g_project_name + '/' + g_out_dir
    p_product_map = out_path_dir + "/prod_mapping"
    
    if g_if_two_source == "False":
        p_raw_data_std = out_path_dir + "/product_mapping_out"
    else:
        p_raw_data_std = out_path_dir + "/raw_data_std"
    
    
    # 输入
    p_max_weight_result = depends_path['max_weight_result']
                
    # 输出
    p_max_result_city = result_path_prefix + g_max_result_city

    # %%
    # # =========== 数据准备，测试用 =============
    def dealIDlength(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字，其他还有包含字母的ID
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df
    
    # 1、df_product_map
    # g_if_two_source == "True"
    df_product_map = spark.read.parquet(p_product_map)
    for i in df_product_map.columns:
        if i in ["标准通用名", "通用名_标准", "药品名称_标准", "S_Molecule_Name"]:
            df_product_map = df_product_map.withColumnRenamed(i, "通用名")
        if i in ["商品名_标准", "S_Product_Name"]:
            df_product_map = df_product_map.withColumnRenamed(i, "标准商品名")
        if i in ["min1_标准"]:
            df_product_map = df_product_map.withColumnRenamed(i, "min2")
        if i in ["标准途径"]:
            df_product_map = df_product_map.withColumnRenamed(i, "std_route")
    if "std_route" not in df_product_map.columns:
            df_product_map = df_product_map.withColumn("std_route", func.lit(''))	
            
    df_product_map = df_product_map.withColumnRenamed('min1', 'MIN') \
                        .withColumnRenamed('min2', 'MIN_STD') \
                        .withColumnRenamed('通用名', 'MOLECULE_STD') \
                        .withColumnRenamed('标准商品名', 'BRAND_STD') \
                        .withColumnRenamed('标准剂型', 'FORM_STD') \
                        .withColumnRenamed('标准规格', 'SPECIFICATIONS_STD') \
                        .withColumnRenamed('标准包装数量', 'PACK_NUMBER_STD') \
                        .withColumnRenamed('标准生产企业', 'MANUFACTURER_STD') \
                        .withColumnRenamed('标准集团', 'CORP_STD') \
                        .withColumnRenamed('std_route', 'ROUTE_STD') \
                        .withColumnRenamed('pfc', 'PACK_ID')
    df_product_map = df_product_map.withColumn('PACK_NUMBER_STD', col('PACK_NUMBER_STD').cast(IntegerType())) \
                            .withColumn('PACK_ID', col('PACK_ID').cast(IntegerType()))
    df_product_map	

    # %%
    # 2、df_product_map
    # g_if_two_source == "True"
    df_cpa_pha_mapping = spark.read.parquet(p_cpa_pha_mapping)
    df_cpa_pha_mapping = df_cpa_pha_mapping.withColumnRenamed('推荐版本', 'COMMEND').where(col("COMMEND") == 1) \
                                    .select('ID', 'PHA').distinct()
    df_cpa_pha_mapping = dealIDlength(df_cpa_pha_mapping)

    # %%
    # 3、market_mapping
    df_market = spark.read.parquet(p_market)
    df_market = df_market.withColumnRenamed("标准通用名", "MOLECULE_STD") \
                            .withColumnRenamed("model", "MARKET") \
                            .withColumnRenamed("mkt", "MARKET") \
                            .select("MARKET", "MOLECULE_STD").distinct()
    if df_market.select("MARKET").dtypes[0][1] == "double":
        df_market = df_market.withColumn("MARKET", col("MARKET").cast(IntegerType()))

    # %%
    # 4、province_city_mapping
    df_province_city_mapping = spark.read.parquet(p_province_city_mapping)
    df_province_city_mapping = df_province_city_mapping.distinct()
    df_province_city_mapping = dealIDlength(df_province_city_mapping)
    df_province_city_mapping = df_province_city_mapping.withColumnRenamed('Procince', 'PROVINCE') \
                                            .withColumnRenamed('City', 'CITY') \
                                            .select('ID', 'PROVINCE', 'CITY')
    # df_province_city_mapping

    # %%
    # 5、province_city_mapping
    df_cpa_pha_mapping_common = spark.read.parquet(p_cpa_pha_mapping_common)
    df_cpa_pha_mapping_common = df_cpa_pha_mapping_common.withColumnRenamed('推荐版本', 'COMMEND') \
                                                .where(col("COMMEND") == 1) \
                                                .withColumnRenamed("PHA", "PHA_COMMON") \
                                                .select("ID", "PHA_COMMON").distinct()
    df_cpa_pha_mapping_common = dealIDlength(df_cpa_pha_mapping_common)

    # %%
    # 6.ID_Bedsize
    df_ID_Bedsize = spark.read.parquet(p_id_bedsize)
    df_ID_Bedsize = dealIDlength(df_ID_Bedsize)
    df_ID_Bedsize = df_ID_Bedsize.withColumnRenamed("Bedsize", "BEDSIZE") \
                           .select("ID", "BEDSIZE").distinct()
    # df_ID_Bedsize

    # %%
    # 7.raw_data
    df_raw_data = spark.read.parquet(p_raw_data_std)
    # === 测试用 =====
    for i in df_raw_data.columns:
        if i in ["数量（支/片）", "最小制剂单位数量", "total_units", "SALES_QTY"]:
            df_raw_data = df_raw_data.withColumnRenamed(i, "Units")
        if i in ["金额（元）", "金额", "sales_value__rmb_", "SALES_VALUE"]:
            df_raw_data = df_raw_data.withColumnRenamed(i, "Sales")
        if i in ["Yearmonth", "YM", "Date", "year_month"]:
            df_raw_data = df_raw_data.withColumnRenamed(i, "Date")
        if i in ["医院编码", "BI_Code", "HOSP_CODE"]:
            df_raw_data = df_raw_data.withColumnRenamed(i, "ID")
            
    for colname, coltype in df_raw_data.dtypes:
        if coltype == "logical":
            df_raw_data = df_raw_data.withColumn(colname, df_raw_data[colname].cast(StringType()))        
    # === 测试用 =====
    
    df_raw_data = df_raw_data.withColumnRenamed('Form', 'FORM') \
                        .withColumnRenamed('Specifications', 'SPECIFICATIONS') \
                        .withColumnRenamed('Pack_Number', 'PACK_NUMBER') \
                        .withColumnRenamed('Manufacturer', 'MANUFACTURER') \
                        .withColumnRenamed('Molecule', 'MOLECULE') \
                        .withColumnRenamed('Source', 'SOURCE') \
                        .withColumnRenamed('Corp', 'CORP') \
                        .withColumnRenamed('Route', 'ROUTE') \
                        .withColumnRenamed('ORG_Measure', 'ORG_MEASURE') \
                        .withColumnRenamed('Sales', 'SALES') \
                        .withColumnRenamed('Units', 'UNITS') \
                        .withColumnRenamed('Units_Box', 'UNITS_BOX') \
                        .withColumnRenamed('Path', 'PATH') \
                        .withColumnRenamed('Sheet', 'SHEET') \
                        .withColumnRenamed('Raw_Hosp_Name', 'RAW_HOSP_NAME') \
                        .withColumnRenamed('Date', 'DATE') \
                        .withColumnRenamed('Brand', 'BRAND')
    df_raw_data = df_raw_data.withColumn('DATE', col('DATE').cast(IntegerType())) \
                        .withColumn('PACK_NUMBER', col('PACK_NUMBER').cast(IntegerType())) \
                        .withColumn('SALES', col('SALES').cast(DoubleType())) \
                        .withColumn('UNITS', col('UNITS').cast(DoubleType())) \
                        .withColumn('UNITS_BOX', col('UNITS_BOX').cast(DoubleType()))
    df_raw_data = dealIDlength(df_raw_data)

    # %%
    # =========== 数据执行 =============
    '''
    合并raw_data 和 max 结果
    旧：双源读取_std文件重新匹配job12的信息，非双源读取job2结果不用重新匹配信息
    新：都统一读取raw，重新匹配
    '''
    # 一. raw文件处理
    if g_if_two_source == "False":
        df_raw_data = df_raw_data.where((col("DATE") >=g_time_left) & (col('DATE') <= g_time_right))
    
    else:
        # 1、匹配产品信息和医院信息
        # job1: df_raw_data 处理，匹配PHA，部分job1
        df_raw_data = df_raw_data.where((col('DATE') >=g_time_left) & (col('DATE') <= g_time_right))
    
        # 匹配：cpa_pha_mapping
        df_raw_data = dealIDlength(df_raw_data)    
        df_raw_data = df_raw_data.join(df_cpa_pha_mapping, on="ID", how="left")
    
        # job2: df_raw_data 处理，生成min1，用product_map 匹配获得min2（MIN_STD），同job2
        # if g_project_name != "Mylan":
        df_raw_data = df_raw_data.withColumn("BRAND", func.when((col('BRAND').isNull()) | (col('BRAND') == 'NA'), col('MOLECULE')).
                                       otherwise(col('BRAND')))
    
        # MIN 生成
        df_raw_data = df_raw_data.withColumn('tmp', func.concat_ws(g_minimum_product_sep, 
                                *[func.when(col(i).isNull(), func.lit("NA")).otherwise(col(i)) for i in g_minimum_product_columns]))
       
        # Mylan不重新生成minimum_product_newname: min1，其他项目生成min1
        # if g_project_name == "Mylan":
        #    df_raw_data = df_raw_data.drop("tmp")
        # else:
        df_raw_data = df_raw_data.withColumnRenamed("tmp", g_minimum_product_newname)
    
        df_product_map_for_rawdata = df_product_map.select("MIN", "MIN_STD", "MOLECULE_STD").distinct()
        
        # 匹配：
        df_raw_data = df_raw_data.join(df_product_map_for_rawdata, on="MIN", how="left")
                                #.drop("MOLECULE_STD") \
                                #.withColumnRenamed("MOLECULE_STD", "MOLECULE_STD")

    # %%
    # 2、匹配：市场名
    df_raw_data = df_raw_data.join(df_market, on="MOLECULE_STD", how="left")
    
    # 3、匹配:通用province_city
    df_raw_data = df_raw_data.join(df_province_city_mapping, on="ID", how="left") \
                            .withColumn("PANEL", func.lit(1))
    
    # 删除医院
    # hospital_ot = spark.read.csv(hospital_ot_path, header=True)
    # df_raw_data = df_raw_data.join(hospital_ot, on="ID", how="left_anti")
    
    # 4、匹配：通用cpa_pha_mapping_common  df_raw_data，当df_raw_data的PHA是空的重新匹配补充
    df_cpa_pha_mapping_common = df_cpa_pha_mapping_common.withColumnRenamed("PHA", "PHA_COMMON")
    df_raw_data = df_raw_data.join(df_cpa_pha_mapping_common, on="ID", how="left")
    df_raw_data = df_raw_data.withColumn("PHA", func.when(col('PHA').isNull(), col('PHA_COMMON')).otherwise(col('PHA'))) \
                                .drop("PHA_COMMON")
    
    # 5、df_raw_data 包含的医院列表
    df_raw_data_PHA = df_raw_data.select("PHA", "DATE").distinct()
    
    # 6、匹配：ID_Bedsize 
    df_raw_data = df_raw_data.join(df_ID_Bedsize, on="ID", how="left")
    
    # 7、筛选：all_models 
    df_raw_data = df_raw_data.where(col('MARKET') == g_market)
    
    # 8、筛选：ID_Bedsize
    if g_project_name != "Janssen":
        if g_bedsize == "True":
            df_raw_data = df_raw_data.where(col('BEDSIZE') > 99)
    
    # 9、计算：groupby        
    if g_hospital_level == "True":
        df_raw_data_city = df_raw_data \
            .groupBy("PROVINCE", "CITY", "DATE", "MIN_STD", "PANEL", "MARKET", "MOLECULE_STD", "PHA") \
            .agg({"SALES":"sum", "UNITS":"sum"}) \
            .withColumnRenamed("sum(SALES)", "PREDICT_SALES") \
            .withColumnRenamed("sum(UNITS)", "PREDICT_UNIT")
    else:
        df_raw_data_city = df_raw_data \
            .groupBy("PROVINCE", "CITY", "DATE", "MIN_STD", "PANEL", "MARKET", "MOLECULE_STD") \
            .agg({"SALES":"sum", "UNITS":"sum"}) \
            .withColumnRenamed("sum(SALES)", "PREDICT_SALES") \
            .withColumnRenamed("sum(UNITS)", "PREDICT_UNIT")

    # %%
    # 二. max文件处理
    #（以前是每个市场进行循环，目前只分析一个市场一个时间单位的数据）
    # df_max_result = spark.read.parquet(p_max_weight_result)
    strcut_type_max_weight_result = StructType([ StructField('PHA', StringType(), True),
                                                    StructField('PROVINCE', StringType(), True),
                                                    StructField('CITY', StringType(), True),
                                                    StructField('DATE', IntegerType(), True),
                                                    StructField('MOLECULE_STD', StringType(), True),
                                                    StructField('MIN_STD', StringType(), True),
                                                    StructField('BEDSIZE', DoubleType(), True),
                                                    StructField('PANEL', DoubleType(), True),
                                                    StructField('SEG', DoubleType(), True),
                                                    StructField('PREDICT_SALES', DoubleType(), True),
                                                    StructField('PREDICT_UNIT', DoubleType(), True) ])
    df_max_result = spark.read.format("parquet").load(p_max_weight_result, schema=strcut_type_max_weight_result)
    # 1、max_result 筛选 BEDSIZE > 99， 且医院不在df_raw_data_PHA 中
    if g_bedsize == "True":
        df_max_result = df_max_result.where(col('BEDSIZE') > 99)
    
    # 2、max 结果中有panel=1和panel=0两部分数据，去掉的数据包括panel=1完全与raw一致的数据，还有panel=0被放大，但是df_raw_data中有真实值的数据
    df_max_result = df_max_result.join(df_raw_data_PHA, on=["PHA", "DATE"], how="left_anti")
    
    if g_hospital_level == "True":
        df_max_result = df_max_result \
            .groupBy("PROVINCE", "CITY", "DATE", "MIN_STD", "PANEL", "MOLECULE_STD", "PHA") \
            .agg({"PREDICT_SALES":"sum", "PREDICT_UNIT":"sum"}) \
            .withColumnRenamed("sum(PREDICT_SALES)", "PREDICT_SALES") \
            .withColumnRenamed("sum(PREDICT_UNIT)", "PREDICT_UNIT")
    else:
        df_max_result = df_max_result \
            .groupBy("PROVINCE", "CITY", "DATE", "MIN_STD", "PANEL", "MOLECULE_STD") \
            .agg({"PREDICT_SALES":"sum", "PREDICT_UNIT":"sum"}) \
            .withColumnRenamed("sum(PREDICT_SALES)", "PREDICT_SALES") \
            .withColumnRenamed("sum(PREDICT_UNIT)", "PREDICT_UNIT")
    
    df_max_result = df_max_result.withColumn("MARKET", func.lit(g_market))
    

    # %%
    # 三. 合并df_raw_data 和 max文件处理
    if g_hospital_level == "True":
        df_raw_data_city = df_raw_data_city.select("PHA", "PROVINCE", "CITY", "DATE", "MIN_STD", "MOLECULE_STD", "PANEL", "MARKET", 
                                                   "PREDICT_SALES", "PREDICT_UNIT")
        max_result_all = df_max_result.select("PHA", "PROVINCE", "CITY", "DATE", "MIN_STD", "MOLECULE_STD", "PANEL", "MARKET", 
                                               "PREDICT_SALES", "PREDICT_UNIT")
    else:
        df_raw_data_city = df_raw_data_city.select("PROVINCE", "CITY", "DATE", "MIN_STD", "MOLECULE_STD", "PANEL", "MARKET", 
                                                   "PREDICT_SALES", "PREDICT_UNIT")
        max_result_all = df_max_result.select("PROVINCE", "CITY", "DATE", "MIN_STD", "MOLECULE_STD", "PANEL", "MARKET", 
                                               "PREDICT_SALES", "PREDICT_UNIT")
    
    max_result_city = max_result_all.union(df_raw_data_city)
    
    # 四. 合并后再进行一次group
    if g_hospital_level == "True":
        max_result_city = max_result_city \
            .groupBy("PROVINCE", "CITY", "DATE", "MIN_STD", "PANEL", "MOLECULE", "PHA", "MARKET") \
            .agg({"PREDICT_SALES":"sum", "PREDICT_UNIT":"sum"}) \
            .withColumnRenamed("sum(PREDICT_SALES)", "PREDICT_SALES") \
            .withColumnRenamed("sum(PREDICT_UNIT)", "PREDICT_UNIT")
        max_result_city = max_result_city.select("PHA", "PROVINCE", "CITY", "DATE", "MIN_STD", "MOLECULE_STD", "PANEL", "MARKET", 
                                                 "PREDICT_SALES", "PREDICT_UNIT")
    else:
        max_result_city = max_result_city \
            .groupBy("PROVINCE", "CITY", "DATE", "MIN_STD", "PANEL", "MOLECULE_STD", "MARKET") \
            .agg({"PREDICT_SALES":"sum", "PREDICT_UNIT":"sum"}) \
            .withColumnRenamed("sum(PREDICT_SALES)", "PREDICT_SALES") \
            .withColumnRenamed("sum(PREDICT_UNIT)", "PREDICT_UNIT")
        max_result_city = max_result_city.select("PROVINCE", "CITY", "DATE", "MIN_STD", "MOLECULE_STD", "PANEL", "MARKET", 
                                                 "PREDICT_SALES", "PREDICT_UNIT")
        
    max_result_city = max_result_city.withColumnRenamed("MOLECULE_STD", "MOLECULE") \
                                    .withColumn("DATE", col("DATE").cast("int"))

    # %%
    # 输出
    max_result_city = max_result_city.repartition(2)
    max_result_city.write.format("parquet") \
        .mode("overwrite").save(p_max_result_city)

    # %%
    # max_result_city.agg(func.sum('PREDICT_SALES'),func.sum('PREDICT_UNIT')).show()

    # %%
    # df=spark.read.parquet('s3a://ph-max-auto/v0.0.1-2020-06-08/贝达/202012_test/MAX_result/MAX_result_202001_202012_city_level/')
    # df.where(df.Date==202012).agg(func.sum('Predict_Sales'),func.sum('Predict_Unit')).show()

