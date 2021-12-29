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
    # extract_path = kwargs['extract_path']
    project_name = kwargs['project_name']
    out_path = kwargs['out_path']
    run_id = kwargs['run_id'].replace(":","_")
    owner = kwargs['owner']
    g_database_temp = kwargs['g_database_temp']
    g_database_input = kwargs['g_database_input']
    ### input args ###
    
    ### output args ###
    # g_out_max_standard = kwargs['g_out_max_standard']
    # g_out_max_standard_brief = kwargs['g_out_max_standard_brief']
    ### output args ###

    
    
    
    
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import json
    import boto3    
    
    # %% 
    # =========== 数据执行 =========== 
    # 输入参数设置
    g_out_max_standard = 'max_result_standard'
    g_out_max_standard_brief = 'max_result_standard_brief'
    
    # dict_input_version = json.loads(g_input_version)
    # print(dict_input_version)
    
    # 输出
    # p_out_max_standard = extract_path + g_out_max_standard + '/project=' + project_name
    # p_out_max_standard_brief = extract_path + g_out_max_standard_brief + '/project=' + project_name
    
    p_tmp_out_max_standard = out_path + g_out_max_standard
    p_tmp_out_max_standard_brief = out_path + g_out_max_standard_brief
    # %% 
    # =========== 输入数据读取 ===========
    def changeColToInt(df, list_cols):
        for i in list_cols:
            df = df.withColumn(i, col(i).cast('int'))
        return df
    def dealToNull(df):
        df = df.replace(["None", ""], None)
        return df
    
    df_max_city_normalize = kwargs['df_province_city_mapping_common']
    df_max_city_normalize = dealToNull(df_max_city_normalize)
    
    df_prod_mapping = kwargs['df_prod_mapping']
    df_prod_mapping = dealToNull(df_prod_mapping)
    
    df_molecule_atc_map = kwargs['df_product_map_all_atc'] 
    df_molecule_atc_map = dealToNull(df_molecule_atc_map)
    
    df_master_data_map = kwargs['df_master_data_map']  
    df_master_data_map = dealToNull(df_master_data_map)
    
    df_max_result_backfill = kwargs['df_max_result_backfill']  
    df_max_result_backfill = dealToNull(df_max_result_backfill)

    # %% 
    # =========== 数据清洗 =============
    logger.debug('数据清洗-start')
    # 函数定义
    def getTrueCol(df, l_colnames, l_df_columns):
        # 检索出正确列名
        l_true_colname = []
        for i in l_colnames:
            if i.lower() in l_df_columns and df.where(~col(i).isNull()).count() > 0:
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
    
    def dealIDLength(df, colname='ID'):
        # ID不足7位的前面补0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字
        # 其他来源的ID 还有包含字母的, 所以要为字符型，不能转为 数值型
        df = df.withColumn(colname, col(colname).cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn(colname, func.regexp_replace(colname, "\\.0", ""))
        df = df.withColumn(colname, func.when(func.length(col(colname)) < 7, func.lpad(col(colname), 6, "0")).otherwise(col(colname)))
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
    df_prod_mapping = dealScheme(df_prod_mapping, {"标准包装数量":"int", "pfc":"int"})
    df_master_data_map = dealScheme(df_master_data_map, {"PACK_ID":"int"})
    df_molecule_atc_map = dealScheme(df_molecule_atc_map, {"PackID":"int"})
    df_max_result_backfill = dealScheme(df_max_result_backfill, {"Date":"int"})
    
    # 3、选择列
    df_prod_mapping = df_prod_mapping.select("min2", "pfc", "通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业").distinct()
    df_max_city_normalize = df_max_city_normalize.select('Province', 'City', '标准省份名称', '标准城市名称').distinct()
    df_master_data_map = df_master_data_map.select("PACK_ID", "MOLE_NAME_CH", "PROD_NAME_CH", "CORP_NAME_CH", "DOSAGE", "SPEC", "PACK", "ATC4_CODE").distinct()
    df_molecule_atc_map = df_molecule_atc_map.select("project", "通用名", "MOLE_NAME_CH", "ATC4_CODE", "ATC3_CODE", "ATC2_CODE", "min2", "PackID").distinct()
    df_max_result_backfill = df_max_result_backfill.drop('version', 'provider', 'owner')
    
    # 4、其他
    # == prod_mapping ==
    # a. pfc为0统一替换为null
    df_prod_mapping = df_prod_mapping.withColumn("pfc", func.when(col('pfc') == 0, None).otherwise(col('pfc'))).distinct()
    # b. min2处理
    df_prod_mapping = df_prod_mapping.withColumnRenamed("pfc", "PACK_ID") \
                                    .withColumn("min2", func.regexp_replace("min2", "&amp;", "&")) \
                                    .withColumn("min2", func.regexp_replace("min2", "&lt;", "<")) \
                                    .withColumn("min2", func.regexp_replace("min2", "&gt;", ">"))

    # %%
    # ========== 数据准备 =========
    
    # mapping用文件：注意各种mapping的去重，唯一匹配
    
    # 1. 城市标准化
    df_max_city_normalize
    
    # 2. master_data_map：PACK_ID - 标准通用名 - ACT, 无缺失
    df_packID_master_map = df_master_data_map.withColumnRenamed("MOLE_NAME_CH", "MOLE_NAME_CH_1") \
                                            .withColumnRenamed("ATC4_CODE", "ATC4_1")
    df_packID_master_map = df_packID_master_map.dropDuplicates(["PACK_ID"])                    
    
    # 3. product_map_all_ATC: 有补充的新的 PACK_ID - 标准通用名 - ACT （0是缺失）
    # 用于补充PACK_ID
    df_add_PACK_ID = df_molecule_atc_map.where(col('project') == project_name) \
                                    .select("min2", "PackID").distinct() \
                                    .withColumn("PackID", func.when(col('PackID') == 0, None).otherwise(col('PackID'))) \
                                    .withColumnRenamed("PackID", "PackID_add") 
    
    # ATC4_CODE 列如果是0，用ATC3_CODE补充，ATC3_CODE 列也是0，用ATC2_CODE补充
    df_molecule_atc_map = df_molecule_atc_map.withColumn('ATC4_CODE', func.when(col('ATC4_CODE') == '0', 
                                                                    func.when(col('ATC3_CODE') == '0', col('ATC2_CODE')) \
                                                                      .otherwise(col('ATC3_CODE'))) \
                                                               .otherwise(col('ATC4_CODE')))
    
    df_molecule_atc_map = df_molecule_atc_map.select("通用名", "MOLE_NAME_CH", "ATC4_CODE") \
                                            .withColumnRenamed("MOLE_NAME_CH", "MOLE_NAME_CH_2") \
                                            .withColumnRenamed("ATC4_CODE", "ATC4_2") \
                                            .dropDuplicates(["通用名"])
    
    df_molecule_atc_map = df_molecule_atc_map.withColumn("MOLE_NAME_CH_2", func.when(col('MOLE_NAME_CH_2') == "0", None).otherwise(col('MOLE_NAME_CH_2'))) \
                        .withColumn("ATC4_2", func.when(col('ATC4_2') == "0", None).otherwise(col('ATC4_2')))
                        
    # 4. 产品信息
    df_product_map = df_prod_mapping.withColumn("project", func.lit(project_name)).distinct()
    # 补充PACK_ID
    df_product_map = df_product_map.join(df_add_PACK_ID, on="min2", how="left")
    df_product_map = df_product_map.withColumn("PACK_ID", 
                            func.when((col('PACK_ID').isNull()) & (~col('PackID_add').isNull()), col('PackID_add')).otherwise(col('PACK_ID'))) \
                            .drop("PackID_add")
    # 去重：保证每个min2只有一条信息, dropDuplicates会取first
    df_product_map = df_product_map.dropDuplicates(["min2"])

    # %%
    # ========== 数据 mapping =========
    
    # 一. max_result 基础信息匹配
    df_max_result = df_max_result_backfill.withColumn("Prod_Name_tmp", col('Prod_Name'))
    df_max_result = df_max_result.withColumn("Prod_Name_tmp", func.regexp_replace("Prod_Name_tmp", "&amp;", "&")) \
                        .withColumn("Prod_Name_tmp", func.regexp_replace("Prod_Name_tmp", "&lt;", "<")) \
                        .withColumn("Prod_Name_tmp", func.regexp_replace("Prod_Name_tmp", "&gt;", ">"))
    if project_name == "Servier":
        df_max_result = df_max_result.withColumn("Prod_Name_tmp", func.regexp_replace("Prod_Name_tmp", "阿托伐他汀\\+齐鲁制药\\(海南\\)有限公司", "美达信"))
    if project_name == "NHWA":
        df_max_result = df_max_result.withColumn("Prod_Name_tmp", func.regexp_replace("Prod_Name_tmp", "迪施宁乳剂", "迪施乐乳剂"))
    
    # product_map 匹配 min2 ：获得 PACK_ID, 通用名, 标准商品名, 标准剂型, 标准规格, 标准包装数量, 标准生产企业
    df_max_result = df_max_result.join(df_product_map, df_max_result["Prod_Name_tmp"] == df_product_map["min2"], how="left") \
                                    .drop("min2","Prod_Name_tmp")

    # %%
    # 二. 标准化
    def getStandard(df):
        '''
        用通用 packID_master_map， molecule_atc_map，max_city_normalize 文件标准化结果
        '''
        # 1. packID_master_map 匹配 PACK_ID ：获得 MOLE_NAME_CH_1, ATC4_1, PROD_NAME_CH, CORP_NAME_CH, DOSAGE, SPEC, PACK
        df_data_standard = df.join(df_packID_master_map, on=["PACK_ID"], how="left")
    
        # 2. molecule_ACT_map 匹配 通用名：获得 MOLE_NAME_CH_2, ATC4_2
        df_data_standard = df_data_standard.join(df_molecule_atc_map, on=["通用名"], how="left")
    
        # 3. 整合 master 匹配结果 和 product_map, molecule_ACT_map 匹配结果
        '''
        ATC4_1 和 MOLE_NAME_CH_1 来自 master 有 pack_id 匹配得到 ; ATC4_2 和 MOLE_NAME_CH_2 来自 molecule_ACT_map 
        '''
        # 3.1 优先使用 ATC4_1，缺失的用 ATC4_2 补充
        df_data_standard = df_data_standard.withColumn("ATC", func.when(col("ATC4_1").isNull(), col("ATC4_2")) \
                                                .otherwise(col("ATC4_1")))
        # 3.2 A10C/D/E是胰岛素, 通用名和公司名用 master来源, 其他信息用product_map来源的                                     
        df_data_standard_yidaosu = df_data_standard.where(func.substring(col('ATC'), 0, 4).isin(['A10C', 'A10D', 'A10E'])) \
                                            .withColumn("PROD_NAME_CH", col('标准商品名')) \
                                            .withColumn("DOSAGE", col('标准剂型')) \
                                            .withColumn("SPEC", col('标准规格')) \
                                            .withColumn("PACK", col('标准包装数量'))
    
        df_data_standard_others = df_data_standard.where((~func.substring(col('ATC'), 0, 4).isin(['A10C', 'A10D', 'A10E'])) | col('ATC').isNull())
    
        # 合并 max_standard_yidaosu 和 max_standard_others
        df_data_standard = df_data_standard_others.union(df_data_standard_yidaosu.select(df_data_standard_others.columns))
    
        # 3.3 master 匹配不上的(ATC4_1是null) 用 molecule_ACT_map 和 product_map 信息
        df_data_standard = df_data_standard.withColumn("标准通用名", func.when(col("MOLE_NAME_CH_1").isNull(), col("MOLE_NAME_CH_2")) \
                                                .otherwise(col("MOLE_NAME_CH_1"))) \
                                .withColumn("标准商品名", func.when(col("ATC4_1").isNull(), col("标准商品名")) \
                                                .otherwise(col("PROD_NAME_CH"))) \
                                .withColumn("标准剂型", func.when(col("ATC4_1").isNull(), col("标准剂型")) \
                                                .otherwise(col("DOSAGE"))) \
                                .withColumn("标准规格", func.when(col("ATC4_1").isNull(), col("标准规格")) \
                                                .otherwise(col("SPEC"))) \
                                .withColumn("标准包装数量", func.when(col("ATC4_1").isNull(), col("标准包装数量")) \
                                                .otherwise(col("PACK"))) \
                                .withColumn("标准生产企业", func.when(col("ATC4_1").isNull(), col("标准生产企业")) \
                                                .otherwise(col("CORP_NAME_CH"))) \
                                .drop("ATC4_1", "ATC4_2", "MOLE_NAME_CH_1", "MOLE_NAME_CH_2")
    
        # 没有标准通用名的 用原始的通用名
        df_data_standard = df_data_standard.withColumn("标准通用名", func.when(col('标准通用名').isNull(), col('通用名')) \
                                                            .otherwise(col('标准通用名')))
    
        # 4. city 标准化：
        '''
        先标准化省，再用(标准省份-City)标准化市
        '''
        df_data_standard = df_data_standard.join(df_max_city_normalize.select("Province", "标准省份名称").distinct(), on=["Province"], how="left")
        df_data_standard = df_data_standard.join(df_max_city_normalize.select("City", "标准省份名称", "标准城市名称").distinct(),
                                on=["标准省份名称", "City"], how="left")
        return df_data_standard
    
    df_max_standard = getStandard(df_max_result)

    # %%
    # ========== 数据结果 =========
    
    # 全量结果汇总
    # df_max_standard_out = df_max_standard.withColumn("project", func.lit(project_name))
    
    df_max_standard_out = df_max_standard.select("Province", "City" ,"Date", "Prod_Name", "Molecule", "PANEL", "DOI", "Predict_Sales", "Predict_Unit", 
                                           "标准通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业", "标准省份名称", "标准城市名称", 
                                            "PACK_ID", "ATC")
    
    
    #df_max_standard_out = df_max_standard_out.withColumn("Date_copy", col('Date'))
        
    # 目录结果汇总,
    df_max_standard_brief = df_max_standard.select("Date", "标准通用名", "ATC", "DOI", "PACK_ID").distinct()
    
    # 列名转为大写
    df_max_standard_out = df_max_standard_out.toDF(*[i.upper() for i in df_max_standard_out.columns])
    df_max_standard_brief = df_max_standard_brief.toDF(*[i.upper() for i in df_max_standard_brief.columns])

    # %%
    # =========== 函数定义：输出结果 =============
    def createPartition(p_out, date):
        # 创建分区
        logger.debug('创建分区')
        Location = p_out + '/version=' + run_id + '/provider=' + project_name + '/owner=' + owner + '/DATE=' + date
        g_out_table = p_out.split('/')[-1]
        
        partition_input_list = [{
         "Values": [run_id, project_name,  owner,  date], 
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
                 .mode("append").partitionBy("version", "provider", "owner", "DATE") \
                 .parquet(p_out)
    
    def outResultForExtract(df, p_out, p_tmp_out, table):
        df = df.withColumn('version', func.lit(run_id)) \
                .withColumn('provider', func.lit(project_name)) \
                .withColumn('owner', func.lit(owner))
        
        # 当期数据包含的时间
        anti_data = df.select('DATE').distinct()
        # 去掉已有数据中重复时间
        try:
            df_old = spark.read.parquet(p_out)
        except:
            df_old = df
               
        df_old_keep = df_old.join(anti_data, on='DATE', how='left_anti') 
        
        # 合并
        df_new = df_old_keep.union(df.select(df_old_keep.columns))
        # 输出临时位置
        df_new.repartition(1).write.format("parquet") \
                 .mode("append").partitionBy("version", "provider", "owner") \
                 .parquet(p_tmp_out)
        
        createPartition(p_tmp_out)
        
        # 重新写出
        df_new = spark.sql("SELECT * FROM %s.%s WHERE version='%s' AND provider='%s' AND  owner='%s'" 
                             %(g_database_temp, table, run_id, project_name, owner))
        df_new = df_new.toDF(*[i.upper() for i in df_new.columns])
        
        df_new.repartition(1).write.format("parquet") \
                 .mode("overwrite").partitionBy("DATE") \
                 .parquet(p_out)

    # %%
    # ========== 数据输出 =========    
    # outResult(df_max_standard_out, p_tmp_out_max_standard)
    # print("输出 max_standard_out：" + p_tmp_out_max_standard)
      
    # outResult(df_max_standard_brief, p_tmp_out_max_standard_brief)
    # print("输出 max_standard_brief：" + p_tmp_out_max_standard_brief)
    
    # pdf = df_max_standard_brief.select('DATE').distinct().toPandas()
    # for indexs in pdf.index:
    #     data = pdf.loc[indexs]
    #     i_data = str(data['DATE'])
    #     createPartition(p_tmp_out_max_standard, i_data)
    #     createPartition(p_tmp_out_max_standard_brief, i_data)
        
    # print('数据执行-Finish')
    
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    df_max_standard_out = lowerColumns(df_max_standard_out)
    
    logger.debug('数据执行-Finish')
    
    return {'out_df':df_max_standard_out}
