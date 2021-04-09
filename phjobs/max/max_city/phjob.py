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
    depend_job_names_keys = kwargs['depend_job_names_keys']
    max_path = kwargs['max_path']
    project_name = kwargs['project_name']
    time_left = kwargs['time_left']
    time_right = kwargs['time_right']
    left_models = kwargs['left_models']
    left_models_time_left = kwargs['left_models_time_left']
    right_models = kwargs['right_models']
    right_models_time_right = kwargs['right_models_time_right']
    all_models = kwargs['all_models']
    if_others = kwargs['if_others']
    out_path = kwargs['out_path']
    out_dir = kwargs['out_dir']
    if_two_source = kwargs['if_two_source']
    hospital_level = kwargs['hospital_level']
    bedsize = kwargs['bedsize']
    id_bedsize_path = kwargs['id_bedsize_path']
    dag_name = kwargs['dag_name']
    run_id = kwargs['run_id']
    g_out_dir = kwargs['g_out_dir']
    ### input args ###
    
    ### output args ###
    c = kwargs['c']
    d = kwargs['d']
    ### output args ###

    from pyspark.sql.types import StringType, IntegerType, DoubleType
    from pyspark.sql import functions as func
    from pyspark.sql.functions import col
    import boto3
    import os    # %%
    '''
    '''
    project_name = "贝达"
    time_left = "202001"
    time_right = "202012"
    all_models = "BD1"
    out_dir = "202012" 
    if_two_source = "True"
    # minimum_product_sep = "\'|\'"
    # %%
    # 输入
    g_minimum_product_columns = g_minimum_product_columns.replace(" ","").split(",")
    if g_minimum_product_sep == "kong":
        g_minimum_product_sep = ""
    
    if if_others == "False":
        if_others = False
    elif if_others == "True":
        if_others = True
    else:
        raise ValueError('if_others: False or True')
    
    if bedsize != "False" and bedsize != "True":
        raise ValueError('bedsize: False or True')
    if hospital_level != "False" and hospital_level != "True":
        raise ValueError('hospital_level: False or True')
    
    if left_models != "Empty":
        left_models = left_models.replace(", ",",").split(",")
    else:
        left_models = []
    
    if right_models != "Empty":
        right_models = right_models.replace(", ",",").split(",")
    else:
        right_models = []
    
    if left_models_time_left == "Empty":
        left_models_time_left = 0
    if right_models_time_right == "Empty":
        right_models_time_right = 0
    
    if all_models != "Empty":
        all_models = all_models.replace(", ",",").split(",")
    else:
        all_models = []
    
    time_left = int(time_left)
    time_right = int(time_right)
    
    province_city_mapping_path = max_path + "/" + project_name + '/province_city_mapping'
    hospital_ot_path = max_path + "/" + project_name + '/hospital_ot.csv'
    p_market = max_path + "/" + project_name + '/mkt_mapping'
    cpa_pha_mapping_path = max_path + "/" + project_name + "/cpa_pha_mapping"
    if id_bedsize_path == 'Empty':
        ID_Bedsize_path = max_path + "/Common_files/ID_Bedsize"
    else:
        ID_Bedsize_path = id_bedsize_path
    
    cpa_pha_mapping_common_path = max_path + "/Common_files/cpa_pha_mapping"
    
    if if_others == True:
        out_dir = out_dir + "/others_box/"
    
    out_path_dir = out_path + "/" + project_name + '/' + out_dir
    product_map_path = out_path_dir + "/prod_mapping"
    if if_two_source == "False":
        raw_data_std_path = out_path_dir + "/product_mapping_out"
    else:
        raw_data_std_path = out_path_dir + "/raw_data_std"
    
    
    # 输入
    p_max_weight_result = depends_path['max_weight_result']
        
            
    # 输出
    '''
    time_range = str(time_left) + '_' + str(time_right)
    tmp_path = out_path_dir + "/MAX_result/tmp"
    if hospital_level == "True" and bedsize == "False":
        max_result_city_csv_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + "_hospital_level_nobed.csv"
        max_result_city_tmp_path = out_path_dir + "/MAX_result/tmp_hospital_nobed_"+ time_range
    elif hospital_level == "True":
        max_result_city_csv_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + "_hospital_level.csv"
        max_result_city_tmp_path = out_path_dir + "/MAX_result/tmp_hospital_"+ time_range
    elif hospital_level == "False" and bedsize == "False":
        max_result_city_csv_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + "_city_level_nobed.csv"
        max_result_city_tmp_path = out_path_dir + "/MAX_result/tmp_city_nobed_"+ time_range
    else:
        max_result_city_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + "_city_level"
        max_result_city_csv_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + "_city_level.csv"
        max_result_city_tmp_path = out_path_dir + "/MAX_result/tmp_city_"+ time_range
    '''
max_result = spark.read.parquet(p_max_weight_result)
max_result
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
# if_two_source == "True"
df_product_map = spark.read.parquet(product_map_path)

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
df_product_map = df_product_map.withColumn('PACK_NUMBER_STD', col('PACK_NUMBER_STD').cast(IntegerType())) \
                        .withColumn('PACK_ID', col('PACK_ID').cast(IntegerType()))
df_product_map

# # product_map
# if "std_route" not in product_map.columns:
#     product_map = product_map.withColumn("std_route", func.lit(''))	
# 2、df_product_map
# if_two_source == "True"
cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
cpa_pha_mapping = cpa_pha_mapping.withColumnRenamed('推荐版本', 'COMMEND').where(col("COMMEND") == 1) \
                                .select('ID', 'PHA').distinct()
cpa_pha_mapping = dealIDlength(cpa_pha_mapping)
# 3、market_mapping
df_market = spark.read.parquet(p_market)
df_markets = df_market.withColumnRenamed("标准通用名", "MOLECULE_STD") \
                        .withColumnRenamed("model", "MARKET") \
                        .select("MARKET", "MOLECULE_STD").distinct()
df_markets
# 4、province_city_mapping
province_city_mapping = spark.read.parquet(province_city_mapping_path)
province_city_mapping = province_city_mapping.distinct()
province_city_mapping = dealIDlength(province_city_mapping)
province_city_mapping = province_city_mapping.withColumnRenamed('Procince', 'PROVINCE') \
                                        .withColumnRenamed('City', 'CITY') \
                                        .select('ID', 'PROVINCE', 'CITY')
province_city_mapping
# 5、province_city_mapping
cpa_pha_mapping_common = spark.read.parquet(cpa_pha_mapping_common_path)
cpa_pha_mapping_common = cpa_pha_mapping_common.withColumnRenamed('推荐版本', 'COMMEND') \
                                            .where(col("COMMEND") == 1) \
                                            .withColumnRenamed("PHA", "PHA_COMMON") \
                                            .select("ID", "PHA_COMMON").distinct()
cpa_pha_mapping_common = dealIDlength(cpa_pha_mapping_common)
# 6.ID_Bedsize
ID_Bedsize = spark.read.parquet(ID_Bedsize_path)
ID_Bedsize = dealIDlength(ID_Bedsize)
ID_Bedsize = ID_Bedsize.withColumnRenamed("Bedsize", "BEDSIZE") \
                       .select("ID", "BEDSIZE").distinct() 
raw_data = spark.read.parquet(raw_data_std_path)
raw_data
# 7.raw_data
df_raw_data = spark.read.parquet(raw_data_std_path)
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
    '''
    if if_two_source == "False":
        df_raw_data = df_raw_data.where((col("DATE") >=time_left) & (col('DATE') <=time_right))
    
    else:
        # job1: df_raw_data 处理，匹配PHA，部分job1
        df_raw_data = df_raw_data.where((col('DATE') >=time_left) & (col('DATE') <=time_right))
    
        # 匹配：cpa_pha_mapping
        df_raw_data = dealIDlength(df_raw_data)    
        df_raw_data = df_raw_data.join(cpa_pha_mapping, on="ID", how="left")
    
        # job2: df_raw_data 处理，生成min1，用product_map 匹配获得min2（MIN_STD），同job2
        if project_name != "Mylan":
            df_raw_data = df_raw_data.withColumn("BRAND", func.when((col('BRAND').isNull()) | (col('BRAND') == 'NA'), col('MOLECULE')).
                                       otherwise(col('BRAND')))
    
        # MIN 生成
        df_raw_data = df_raw_data.withColumn('tmp', func.concat_ws(g_minimum_product_sep, 
                                *[func.when(col(i).isNull(), func.lit("NA")).otherwise(col(i)) for i in g_minimum_product_columns]))
       
        # Mylan不重新生成minimum_product_newname: min1，其他项目生成min1
        if project_name == "Mylan":
            df_raw_data = df_raw_data.drop("tmp")
        else:
            df_raw_data = df_raw_data.withColumnRenamed("tmp", g_minimum_product_newname)
    
        product_map_for_rawdata = df_product_map.select("MIN", "MIN_STD", "MOLECULE_STD").distinct()
        
        # 匹配：
        df_raw_data = df_raw_data.join(product_map_for_rawdata, on="MIN", how="left") \
                                .drop("MOLECULE_STD") \
                                .withColumnRenamed("MOLECULE_STD", "MOLECULE_STD")

# df_raw_data = df_raw_data.withColumnRenamed("mkt", "DOI") \
#             .withColumnRenamed("min2", "MIN_STD") \
#             .withColumnRenamed("year_month", "DATE") \
#             .select("ID", "DATE", "MIN_STD", "Sales", "Units", "DOI", "PHA", "MOLECULE_STD")
# df_raw_data = dealIDlength(df_raw_data)
# df_raw_data

# 匹配市场名
df_raw_data = df_raw_data.join(df_market, df_raw_data["MOLECULE_STD"] == df_market["MOLECULE_STD"], how="left")

# 匹配:通用province_city
df_raw_data = df_raw_data.join(province_city_mapping, on="ID", how="left") \
                        .withColumn("PANEL", func.lit(1))

# 删除医院
# hospital_ot = spark.read.csv(hospital_ot_path, header=True)
# df_raw_data = df_raw_data.join(hospital_ot, on="ID", how="left_anti")

# 匹配：通用cpa_pha_mapping_commondf_raw_data，当df_raw_data的PHA是空的重新匹配补充
cpa_pha_mapping_common = cpa_pha_mapping_common.withColumnRenamed("PHA", "PHA_COMMON")
df_raw_data = df_raw_data.join(cpa_pha_mapping_common, on="ID", how="left")
df_raw_data = df_raw_data.withColumn("PHA", func.when(col('PHA').isNull(), col('PHA_COMMON')).otherwise(col('PHA'))) \
                            .drop("PHA_COMMON")

# df_raw_data 包含的医院列表
df_raw_data_PHA = df_raw_data.select("PHA", "DATE").distinct()

# 匹配：ID_Bedsize 
df_raw_data = df_raw_data.join(ID_Bedsize, on="ID", how="left")

# 筛选：all_models 
if df_raw_data.select("DOI").dtypes[0][1] == "double":
    df_raw_data = df_raw_data.withColumn("DOI", col("DOI").cast(IntegerType()))
df_raw_data = df_raw_data.where(col('DOI').isin(all_models))

# 筛选：ID_Bedsize
if project_name != "Janssen":
    if bedsize == "True":
        df_raw_data = df_raw_data.where(df_raw_data.Bedsize > 99)

# 计算：groupby        
if hospital_level == "True":
    df_raw_data_city = df_raw_data \
        .groupBy("PROVINCE", "CITY", "DATE", "MIN_STD", "PANEL", "DOI", "MOLECULE_STD", "PHA") \
        .agg({"SALES":"sum", "UNITS":"sum"}) \
        .withColumnRenamed("sum(SALES)", "PREDICT_SALES") \
        .withColumnRenamed("sum(UNITS)", "PREDICT_UNIT") \
        .withColumnRenamed("MOLECULE_STD", "MOLECULE") 
else:
    df_raw_data_city = df_raw_data \
        .groupBy("PROVINCE", "CITY", "DATE", "MIN_STD", "PANEL", "DOI", "MOLECULE_STD") \
        .agg({"SALES":"sum", "UNITS":"sum"}) \
        .withColumnRenamed("sum(SALES)", "PREDICT_SALES") \
        .withColumnRenamed("sum(UNITS)", "PREDICT_UNIT") \
        .withColumnRenamed("MOLECULE_STD", "MOLECULE") 
    # %%
    # 2. max文件处理
    #（以前是每个市场进行循环，目前只分析一个市场一个时间单位的数据）
    
    max_result = spark.read.parquet(p_max_weight_result)
    
    index = 0
    for market in all_models:
        # market 的 time_left 和 time_right 选择，默认为参数时间
        if market in left_models:
            time_left_1 = left_models_time_left
        else:
            time_left_1 = time_left
        if market in right_models:
            time_right_1 = right_models_time_right
        else:
            time_right_1 = time_right
    
        time_range = str(time_left_1) + '_' + str(time_right_1)
    
        if if_others:
            max_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + '_'  + market + "_hosp_level_box"
        else:
            max_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + '_' + market + "_hosp_level"
    
        max_result = spark.read.parquet(max_path)
    
        # max_result 筛选 BEDSIZE > 99， 且医院不在df_raw_data_PHA 中
        if bedsize == "True":
            max_result = max_result.where(max_result.BEDSIZE > 99)
            
        # max 结果中有panel=1和panel=0两部分数据，去掉的数据包括panel=1完全与raw一致的数据，还有panel=0被放大，但是df_raw_data中有真实值的数据
        max_result = max_result.join(df_raw_data_PHA, on=["PHA", "DATE"], how="left_anti")
    
        if hospital_level == "True":
            max_result = max_result \
                .groupBy("PROVINCE", "CITY", "DATE", "MIN_STD", "PANEL", "MOLECULE", "PHA") \
                .agg({"PREDICT_SALES":"sum", "PREDICT_UNIT":"sum"}) \
                .withColumnRenamed("sum(PREDICT_SALES)", "PREDICT_SALES") \
                .withColumnRenamed("sum(PREDICT_UNIT)", "PREDICT_UNIT")
        else:
            max_result = max_result \
                .groupBy("PROVINCE", "CITY", "DATE", "MIN_STD", "PANEL", "MOLECULE") \
                .agg({"PREDICT_SALES":"sum", "PREDICT_UNIT":"sum"}) \
                .withColumnRenamed("sum(PREDICT_SALES)", "PREDICT_SALES") \
                .withColumnRenamed("sum(PREDICT_UNIT)", "PREDICT_UNIT")
    
        max_result = max_result.withColumn("DOI", func.lit(market))
    
    
        if index ==0:
            # max_result_all = max_result
            max_result = max_result.repartition(1)
            max_result.write.format("parquet") \
                .mode("overwrite").save(max_result_city_tmp_path)
    
        else:
            # max_result_all = max_result_all.union(max_result)
            max_result = max_result.repartition(1)
            max_result.write.format("parquet") \
                .mode("append").save(max_result_city_tmp_path)
    
        index = index + 1
    
    max_result_all = spark.read.parquet(max_result_city_tmp_path)
    # %%
    # 3. 合并df_raw_data 和 max文件处理
    if hospital_level == "True":
        df_raw_data_city = df_raw_data_city.select("PHA", "PROVINCE", "CITY", "DATE", "MIN_STD", "MOLECULE", "PANEL", "DOI", 
                                                   "PREDICT_SALES", "PREDICT_UNIT")
        max_result_all = max_result_all.select("PHA", "PROVINCE", "CITY", "DATE", "MIN_STD", "MOLECULE", "PANEL", "DOI", 
                                               "PREDICT_SALES", "PREDICT_UNIT")
    else:
        df_raw_data_city = df_raw_data_city.select("PROVINCE", "CITY", "DATE", "MIN_STD", "MOLECULE", "PANEL", "DOI", 
                                                   "PREDICT_SALES", "PREDICT_UNIT")
        max_result_all = max_result_all.select("PROVINCE", "CITY", "DATE", "MIN_STD", "MOLECULE", "PANEL", "DOI", 
                                               "PREDICT_SALES", "PREDICT_UNIT")
    
    max_result_city = max_result_all.union(df_raw_data_city)
    
    # 4. 合并后再进行一次group
    if hospital_level == "True":
        max_result_city = max_result_city \
            .groupBy("PROVINCE", "CITY", "DATE", "MIN_STD", "PANEL", "MOLECULE", "PHA", "DOI") \
            .agg({"PREDICT_SALES":"sum", "PREDICT_UNIT":"sum"}) \
            .withColumnRenamed("sum(PREDICT_SALES)", "PREDICT_SALES") \
            .withColumnRenamed("sum(PREDICT_UNIT)", "PREDICT_UNIT")
        max_result_city = max_result_city.select("PHA", "PROVINCE", "CITY", "DATE", "MIN_STD", "MOLECULE", "PANEL", "DOI", 
                                                 "PREDICT_SALES", "PREDICT_UNIT")
    else:
        max_result_city = max_result_city \
            .groupBy("PROVINCE", "CITY", "DATE", "MIN_STD", "PANEL", "MOLECULE", "DOI") \
            .agg({"PREDICT_SALES":"sum", "PREDICT_UNIT":"sum"}) \
            .withColumnRenamed("sum(PREDICT_SALES)", "PREDICT_SALES") \
            .withColumnRenamed("sum(PREDICT_UNIT)", "PREDICT_UNIT")
        max_result_city = max_result_city.select("PROVINCE", "CITY", "DATE", "MIN_STD", "MOLECULE", "PANEL", "DOI", 
                                                 "PREDICT_SALES", "PREDICT_UNIT")
    # %%
    # 5.输出判断是否已有 max_result_city_path 结果
    '''
    如果已经存在 max_result_city_path 则用新的结果对已有结果进行DOI替换和补充
    '''
    file_name = max_result_city_path.replace('//', '/').split('s3a:/ph-max-auto/')[1]
    
    s3 = boto3.resource('s3', region_name='cn-northwest-1',
                            aws_access_key_id="AKIAWPBDTVEAEU44ZAGT",
                            aws_secret_access_key="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")
    bucket = s3.Bucket('ph-max-auto')
    judge = 0
    for obj in bucket.objects.filter(Prefix = file_name):
        path, filename = os.path.split(obj.key)  
        if path == file_name:
            judge += 1
            
    if judge > 0:
        old_max_out = spark.read.parquet(max_result_city_path)   
        new_markets = max_result_city.select('DOI').distinct().toPandas()['DOI'].tolist()    
        old_max_out_keep = old_max_out.where(~old_max_out['DOI'].isin(new_markets))    
        max_result_city_final = max_result_city.union(old_max_out_keep.select(max_result_city.columns))
        # 中间文件读写一下
        max_result_city_final = max_result_city_final.repartition(2)
        max_result_city_final.write.format("parquet") \
                            .mode("overwrite").save(tmp_path)
        max_result_city_final = spark.read.parquet(tmp_path)   
    else:
        max_result_city_final = max_result_city.repartition(2)
    # %%
    # max_result_city_final.groupby('PANEL').agg(func.sum('PREDICT_SALES')).show()
    # %%
    # hospital_level 的只输出csv
    if hospital_level == "False" and bedsize == "True":     
        max_result_city_final = max_result_city_final.repartition(2)
        max_result_city_final.write.format("parquet") \
            .mode("overwrite").save(max_result_city_path)
    
    max_result_city_final = max_result_city_final.repartition(1)
    max_result_city_final.write.format("csv").option("header", "true") \
        .mode("overwrite").save(max_result_city_csv_path)
