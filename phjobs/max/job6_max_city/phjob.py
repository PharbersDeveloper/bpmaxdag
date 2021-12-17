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
    project_name = kwargs['project_name']
    time_left = kwargs['time_left']
    time_right = kwargs['time_right']
    all_models = kwargs['all_models']
    if_others = kwargs['if_others']
    minimum_product_columns = kwargs['minimum_product_columns']
    minimum_product_sep = kwargs['minimum_product_sep']
    minimum_product_newname = kwargs['minimum_product_newname']
    if_two_source = kwargs['if_two_source']
    hospital_level = kwargs['hospital_level']
    bedsize = kwargs['bedsize']
    id_bedsize_path = kwargs['id_bedsize_path']
    out_path = kwargs['out_path']
    run_id = kwargs['run_id']
    owner = kwargs['owner']
    g_database_temp = kwargs['g_database_temp']
    g_database_input = kwargs['g_database_input']
    ### input args ###
    
    ### output args ###
    g_out_max_backfill = kwargs['g_out_max_backfill']
    ### output args ###

    
    
    
    
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StringType, IntegerType, DoubleType
    from pyspark.sql import functions as func
    from pyspark.sql.functions import col
    import boto3
    import os   
    import pandas as pd   
    import json
    import boto3
    # %%
    # project_name = 'Empty'
    # time_left = 'Empty'
    # time_right = 'Empty'
    # all_models = 'Empty'
    # if_others = 'False'
    # minimum_product_columns = 'Brand, Form, Specifications, Pack_Number, Manufacturer'
    # minimum_product_sep = '|'
    # minimum_product_newname = 'min1'
    # if_two_source = 'False'
    # hospital_level = 'False'
    # bedsize = 'True'
    # id_bedsize_path = 'Empty'

    # %% 
    # 输入参数设置
    g_out_max_backfill = 'max_result_backfill'
    minimum_product_columns = minimum_product_columns.replace(" ","").split(",")
    if minimum_product_sep == "kong":
        minimum_product_sep = ""
    
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
        
    if all_models != "Empty":
        all_models = all_models.replace(" ",",").split(",")
    else:
        all_models = []
        
    time_left = int(time_left)
    time_right = int(time_right)
    
    # dict_input_version = json.loads(g_input_version)
    # print(dict_input_version)
    
    if if_two_source == "False":
        g_raw_data_type = "df_raw_data"
    else:
        g_raw_data_type = "df_raw_data_std"
            
    # 输出
    p_out_max_backfill = out_path + g_out_max_backfill

    # %% 
    # 输入数据读取
    df_province_city_mapping =  kwargs["df_province_city_mapping"]

    df_mkt_mapping =  kwargs["df_mkt_mapping"]

    df_cpa_pha_mapping =  kwargs["df_cpa_pha_mapping"]

    df_cpa_pha_mapping_common =  kwargs["df_cpa_pha_mapping_common"]

    df_id_bedsize =  kwargs["df_id_bedsize"]

    df_prod_mapping =  kwargs["df_prod_mapping"]

    df_raw_data = kwargs[g_raw_data_type]

    df_max_result_raw = kwargs["df_max_result_raw"]

    # %% 
    # =========== 数据清洗 =============
    logger.debug('数据清洗-start')
    # 函数定义
    def getTrueCol(df, l_colnames, l_df_columns):
        # 检索出正确列名
        l_true_colname = []
        for i in l_colnames:
            if i.lower() in l_df_columns and df.where( (~col(i).isNull()) & (col(i) != 'None') ).count() > 0:
                l_true_colname.append(i)
        if len(l_true_colname) > 1:
           raise ValueError('有重复列名: %s' %(l_true_colname))
        if len(l_true_colname) == 0:
           raise ValueError('缺少列信息: %s' %(l_colnames)) 
        return l_true_colname[0]  
    
    def getTrueColRenamed(df, dict_cols, l_df_columns):
        # 对列名重命名
        for i in dict_cols.keys():
            true_colname = getTrueCol(df, dict_cols[i], l_df_columns)
            logger.debug(true_colname)
            if true_colname != i:
                if i in l_df_columns:
                    # 删除原表中已有的重复列名
                    df = df.drop(i)
                df = df.withColumnRenamed(true_colname, i)
        return df
    
    def dealScheme(df, dict_scheme):
        # 数据类型处理
        for i in dict_scheme.keys():
            df = df.withColumn(i, col(i).cast(dict_scheme[i]))
        return df
    
    def deal_ID_length(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字，其他还有包含字母的ID
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df
    
    # 1、列名清洗
    # 待清洗列名
    dict_cols_prod_map = {"通用名":["通用名", "标准通用名", "通用名_标准", "药品名称_标准", "S_Molecule_Name"], 
                          "min2":["min2", "min1_标准"],
                          "pfc":["pfc", "packcode", "Pack_ID", "PackID", "packid"],
                          "标准商品名":["标准商品名", "商品名_标准", "S_Product_Name"],
                          "标准剂型":["标准剂型", "剂型_标准", "Form_std", "S_Dosage"],
                          "标准规格":["标准规格", "规格_标准", "Specifications_std", "药品规格_标准", "S_Pack"],
                          "标准包装数量":["标准包装数量", "包装数量2", "包装数量_标准", "Pack_Number_std", "S_PackNumber", "最小包装数量"],
                          "标准生产企业":["标准生产企业", "标准企业", "生产企业_标准", "Manufacturer_std", "S_CORPORATION", "标准生产厂家"]
                         }
    #  执行
    df_prod_mapping = getTrueColRenamed(df_prod_mapping, dict_cols_prod_map, df_prod_mapping.columns)
    
    # 2、数据类型处理
    dict_scheme_raw_data = {"Date":"int", "Sales":"double", "Units":"double", "Units_Box":"double", "Pack_Number":"int"}
    df_raw_data = dealScheme(df_raw_data, dict_scheme_raw_data)
    dict_scheme_prod_mapping = {"标准包装数量":"int", "pfc":"int"}
    df_prod_mapping = dealScheme(df_prod_mapping, dict_scheme_prod_mapping)
    
    # 3、选择标准列
    # 标准列名
    std_cols_raw_data = ['Date', 'ID', 'Raw_Hosp_Name', 'Brand', 'Form', 'Specifications', 'Pack_Number', 'Manufacturer', 'Molecule', 'Source', 'Corp', 
                     'Route', 'ORG_Measure', 'Sales', 'Units', 'Units_Box', 'Path', 'Sheet']
    #  执行
    df_prod_mapping = df_prod_mapping.select("min1", "min2", "通用名").distinct()
    df_raw_data = df_raw_data.select(std_cols_raw_data)
    df_cpa_pha_mapping = df_cpa_pha_mapping.select('ID', 'PHA', '推荐版本').distinct()
    df_cpa_pha_mapping_common = df_cpa_pha_mapping_common.select('ID', 'PHA', '推荐版本').distinct()
    df_mkt_mapping = df_mkt_mapping.select('mkt', '标准通用名').distinct() \
                                   .withColumnRenamed("标准通用名", "S_Molecule")
    df_province_city_mapping = df_province_city_mapping.select('id', 'province', 'city').distinct()
    df_id_bedsize = df_id_bedsize.select('id', 'bedsize').distinct() \
                                .withColumn("bedsize", col('bedsize').cast('double'))
    df_max_result_raw = df_max_result_raw.drop('version', 'provider', 'owner') \
                                        .withColumn("panel", col('panel').cast('int'))
                            
    
    # 4、ID列补位
    df_raw_data = deal_ID_length(df_raw_data)
    df_cpa_pha_mapping = deal_ID_length(df_cpa_pha_mapping)
    df_cpa_pha_mapping_common = deal_ID_length(df_cpa_pha_mapping_common)
    df_province_city_mapping = deal_ID_length(df_province_city_mapping)
    df_id_bedsize = deal_ID_length(df_id_bedsize)
    
    # 5、其他处理
    if df_raw_data.where( (~col('Pack_Number').isNull()) & (col('Pack_Number') != 'None') ).count() == 0:
        df_raw_data = df_raw_data.withColumn("Pack_Number", func.lit(0))

    # %%
    # =========== 函数定义：输出结果 =============
    def createPartition(p_out):
        # 创建分区
        logger.debug('创建分区')
        Location = p_out + '/version=' + run_id + '/provider=' + project_name + '/owner=' + owner
        g_out_table = p_out.split('/')[-1]
        
        partition_input_list = [{
         "Values": [run_id, project_name,  owner], 
        "StorageDescriptor": {
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
            }, 
            "Location": Location, 
        } 
            }]    
        client = boto3.client('glue', region_name='cn-northwest-1')
        glue_info = client.batch_create_partition(DatabaseName=g_database_temp, TableName=g_out_table, PartitionInputList=partition_input_list)
        logger.debug(glue_info)
        
    def outResult(df, p_out):
        df = df.withColumn('version', func.lit(run_id)) \
                .withColumn('provider', func.lit(project_name)) \
                .withColumn('owner', func.lit(owner))
        df.repartition(1).write.format("parquet") \
                 .mode("append").partitionBy("version", "provider", "owner") \
                 .parquet(p_out)

    # %%
    # =========== 数据执行 =============
    '''
    合并raw_data 和 max 结果
    '''
    
    # 一. raw_data 处理，用于数据回填
    df_raw_data_map = df_raw_data.where((col('Date') >=time_left) & (col('Date') <=time_right)) \
                            .withColumn("PANEL", func.lit(1))
    
    # 1 信息匹配
    # 1.1 匹配 pha
    df_cpa_pha_mapping = df_cpa_pha_mapping.where(col("推荐版本") == 1).select("ID", "PHA").distinct()
    df_raw_data_map = df_raw_data_map.join(df_cpa_pha_mapping, on='ID', how="left")
    
    # 1.2 匹配 产品信息
    df_raw_data_map = df_raw_data_map.withColumn("Brand", func.when( (col('Brand') == 'None') | (col('Brand') == 'NA'), col('Molecule')). \
                                             otherwise(col('Brand')))
    # min1 生成
    df_raw_data_map = df_raw_data_map.withColumn('Pack_Number', col('Pack_Number').cast(StringType()))
    df_raw_data_map = df_raw_data_map.withColumn(minimum_product_newname, func.concat_ws(minimum_product_sep, 
                                    *[func.when(col(i) == 'None', func.lit("NA")).otherwise(col(i)) for i in minimum_product_columns]))
    
    df_raw_data_map = df_raw_data_map.join(df_prod_mapping, on="min1", how="left") \
                            .withColumnRenamed("通用名", "S_Molecule")
    
    # 1.3 匹配市场名
    df_raw_data_map = df_raw_data_map.join(df_mkt_mapping, on='S_Molecule', how="left")
    
    
    # 列重命名
    df_raw_data_map = df_raw_data_map.withColumnRenamed("mkt", "DOI") \
                        .withColumnRenamed("min2", "Prod_Name") \
                        .select("ID", "Date", "Prod_Name", "Sales", "Units", "DOI", "PHA", "S_Molecule", "PANEL")
    
    # 1.4 匹配城市信息
    df_raw_data_map = df_raw_data_map.join(df_province_city_mapping, on="ID", how="left")
                        
    # 1.5 raw_data PHA是空的重新匹配
    df_cpa_pha_mapping_common = df_cpa_pha_mapping_common.where(col("推荐版本") == 1) \
                                                        .withColumnRenamed("PHA", "PHA_common") \
                                                        .select("ID", "PHA_common").distinct()
    df_raw_data_map = df_raw_data_map.join(df_cpa_pha_mapping_common, on="ID", how="left")
    df_raw_data_map = df_raw_data_map.withColumn("PHA", func.when(col('PHA') == 'None', col('PHA_common')).otherwise(col('PHA'))) \
                                .drop("PHA_common")
    
    # raw_data 医院列表
    df_raw_data_PHA = df_raw_data_map.select("PHA", "Date").distinct()
    
    # 1.6 ID_Bedsize 匹配
    df_raw_data_map = df_raw_data_map.join(df_id_bedsize, on="ID", how="left")

    # %%
    # 2. 计算
    # all_models 筛选
    if df_raw_data_map.select("DOI").dtypes[0][1] == "double":
        df_raw_data_map = df_raw_data_map.withColumn("DOI", col("DOI").cast(IntegerType()))
    df_raw_data_map = df_raw_data_map.where(col('DOI').isin(all_models))
    
    if bedsize == "True":
        df_raw_data_map = df_raw_data_map.where(col('Bedsize') > 99)
    
    if hospital_level == "True":
        df_raw_data_agg = df_raw_data_map \
            .groupBy("Province", "City", "Date", "Prod_Name", "PANEL", "DOI", "S_Molecule", "PHA") \
            .agg({"Sales":"sum", "Units":"sum"}) \
            .withColumnRenamed("sum(Sales)", "Predict_Sales") \
            .withColumnRenamed("sum(Units)", "Predict_Unit") \
            .withColumnRenamed("S_Molecule", "Molecule") 
    else:
        df_raw_data_agg = df_raw_data_map \
            .groupBy("Province", "City", "Date", "Prod_Name", "PANEL", "DOI", "S_Molecule") \
            .agg({"Sales":"sum", "Units":"sum"}) \
            .withColumnRenamed("sum(Sales)", "Predict_Sales") \
            .withColumnRenamed("sum(Units)", "Predict_Unit") \
            .withColumnRenamed("S_Molecule", "Molecule") 

    # %%
    # 二. max文件处理
    # max_result 筛选 BEDSIZE > 99， 且医院不在 raw_data_PHA 中
    if bedsize == "True":
        df_max_result_raw = df_max_result_raw.where(col('BEDSIZE') > 99)
    df_max_result_raw = df_max_result_raw.join(df_raw_data_PHA, on=["PHA", "Date"], how="left_anti")
    
    if hospital_level == "True":
        df_max_result_agg = df_max_result_raw \
            .groupBy("Province", "City", "Date", "Prod_Name", "PANEL", "Molecule", "PHA", "DOI") \
            .agg({"Predict_Sales":"sum", "Predict_Unit":"sum"}) \
            .withColumnRenamed("sum(Predict_Sales)", "Predict_Sales") \
            .withColumnRenamed("sum(Predict_Unit)", "Predict_Unit")
    else:
        df_max_result_agg = df_max_result_raw \
            .groupBy("Province", "City", "Date", "Prod_Name", "PANEL", "Molecule", "DOI") \
            .agg({"Predict_Sales":"sum", "Predict_Unit":"sum"}) \
            .withColumnRenamed("sum(Predict_Sales)", "Predict_Sales") \
            .withColumnRenamed("sum(Predict_Unit)", "Predict_Unit")

    # %%
    # 三. 合并raw_data 和 max文件处理
    if hospital_level == "True":
        df_raw_data_agg = df_raw_data_agg.select("PHA", "Province", "City", "Date", "Prod_Name", "Molecule", "PANEL", "DOI", "Predict_Sales", "Predict_Unit")
        df_max_result_agg = df_max_result_agg.select("PHA", "Province", "City", "Date", "Prod_Name", "Molecule", "PANEL", "DOI", "Predict_Sales", "Predict_Unit")
    else:
        df_raw_data_agg = df_raw_data_agg.select("Province", "City", "Date", "Prod_Name", "Molecule", "PANEL", "DOI", "Predict_Sales", "Predict_Unit")
        df_max_result_agg = df_max_result_agg.select("Province", "City", "Date", "Prod_Name", "Molecule", "PANEL", "DOI", "Predict_Sales", "Predict_Unit")
    
    df_max_result_backfill = df_max_result_agg.union(df_raw_data_agg)
    
    # 合并后再进行一次group
    if hospital_level == "True":
        df_max_result_backfill = df_max_result_backfill \
            .groupBy("Province", "City", "Date", "Prod_Name", "PANEL", "Molecule", "PHA", "DOI") \
            .agg({"Predict_Sales":"sum", "Predict_Unit":"sum"}) \
            .withColumnRenamed("sum(Predict_Sales)", "Predict_Sales") \
            .withColumnRenamed("sum(Predict_Unit)", "Predict_Unit")
        df_max_result_backfill = df_max_result_backfill.select("PHA", "Province", "City", "Date", "Prod_Name", "Molecule", "PANEL", "DOI", "Predict_Sales", "Predict_Unit")
    else:
        df_max_result_backfill = df_max_result_backfill \
            .groupBy("Province", "City", "Date", "Prod_Name", "PANEL", "Molecule", "DOI") \
            .agg({"Predict_Sales":"sum", "Predict_Unit":"sum"}) \
            .withColumnRenamed("sum(Predict_Sales)", "Predict_Sales") \
            .withColumnRenamed("sum(Predict_Unit)", "Predict_Unit")
        df_max_result_backfill = df_max_result_backfill.select("Province", "City", "Date", "Prod_Name", "Molecule", "PANEL", "DOI", "Predict_Sales", "Predict_Unit")

    # %%
    # ==== **** 输出补数结果 **** ==== 
    # outResult(df_max_result_backfill, p_out_max_backfill)
    # print("输出 raw_data_adding_final：" + p_out_max_backfill)
    # createPartition(p_out_max_backfill)
    # print('数据执行-Finish')
    
    # %%
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    df_max_result_backfill = lowerColumns(df_max_result_backfill)
    
    logger.debug('数据执行-Finish')
    
    return {'out_df':df_max_result_backfill}
