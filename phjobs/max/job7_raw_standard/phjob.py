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
    if_two_source = kwargs['if_two_source']
    minimum_product_sep = kwargs['minimum_product_sep']
    minimum_product_columns = kwargs['minimum_product_columns']
    # g_for_extract = kwargs['g_for_extract']
    out_path = kwargs['out_path']
    run_id = kwargs['run_id']
    owner = kwargs['owner']
    g_database_temp = kwargs['g_database_temp']
    g_database_input = kwargs['g_database_input']
    ### input args ###
    
    ### output args ###
    # g_out_raw_standard = kwargs['g_out_raw_standard']
    # g_out_raw_standard_brief = kwargs['g_out_raw_standard_brief']
    ### output args ###

    
    
    
    
    
    
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col 
    import json
    import boto3    # %% 
    # 输入参数设置
    g_out_raw_standard = 'raw_data_standard'
    g_out_raw_standard_brief = 'raw_data_standard_brief'
    # dict_input_version = json.loads(g_input_version)
    # print(dict_input_version)
    
    if minimum_product_sep == 'kong':
        minimum_product_sep = ''
    minimum_product_columns = minimum_product_columns.replace(' ', '').split(',')
    
    # 输出
    p_out_raw_standard = out_path + g_out_raw_standard
    p_out_raw_standard_brief = out_path + g_out_raw_standard_brief
    # %% 
    # 输入数据读取
    df_max_city_normalize =  kwargs['df_province_city_mapping_common']

    df_prod_mapping =  kwargs['df_prod_mapping']

    df_molecule_atc_map =  kwargs['df_product_map_all_atc']

    df_master_data_map =  kwargs['df_master_data_map']

    df_cpa_pha_mapping =  kwargs['df_cpa_pha_mapping_common']

    df_mkt_mapping = kwargs['df_mkt_mapping']

    df_universe =  kwargs['df_universe_base_common']

    df_raw_data = kwargs['df_raw_data_delivery']
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
    df_raw_data = dealScheme(df_raw_data, {"Date":"int", "Sales":"double", "Units":"double", "Units_Box":"double", "Pack_Number":"int"})
        
    # 3、选择列
    df_prod_mapping = df_prod_mapping.select("min1", "min2", "pfc", "通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业").distinct()
    df_max_city_normalize = df_max_city_normalize.select('Province', 'City', '标准省份名称', '标准城市名称').distinct()
    df_master_data_map = df_master_data_map.select("PACK_ID", "MOLE_NAME_CH", "PROD_NAME_CH", "CORP_NAME_CH", "DOSAGE", "SPEC", "PACK", "ATC4_CODE").distinct()
    df_molecule_atc_map = df_molecule_atc_map.select("project", "通用名", "MOLE_NAME_CH", "ATC4_CODE", "ATC3_CODE", "ATC2_CODE", "min2", "PackID").distinct()
    df_cpa_pha_mapping = df_cpa_pha_mapping.select('ID', 'PHA', '推荐版本').distinct()
    df_universe = df_universe.select('新版ID', '新版名称', 'Province', 'City').distinct()
    df_mkt_mapping = df_mkt_mapping.select('mkt', '标准通用名').distinct()
    
    std_cols_raw_data = ['Date', 'ID', 'Raw_Hosp_Name', 'Brand', 'Form', 'Specifications', 'Pack_Number', 'Manufacturer', 'Molecule', 'Source', 'Corp', 
                         'Route', 'ORG_Measure', 'Sales', 'Units', 'Units_Box', 'Path', 'Sheet']
    df_raw_data = df_raw_data.select(std_cols_raw_data)
    
    # 4、其他
    # ** prod_mapping
    # a. pfc为0统一替换为null
    df_prod_mapping = df_prod_mapping.withColumn("pfc", func.when(col('pfc') == 0, None).otherwise(col('pfc'))).distinct()
    # b. min2处理
    df_prod_mapping = df_prod_mapping.withColumnRenamed("pfc", "PACK_ID") \
                                    .withColumn("min2", func.regexp_replace("min2", "&amp;", "&")) \
                                    .withColumn("min2", func.regexp_replace("min2", "&lt;", "<")) \
                                    .withColumn("min2", func.regexp_replace("min2", "&gt;", ">"))
    # ** raw_data ==
    if df_raw_data.where( (~col('Pack_Number').isNull()) & (col('Pack_Number') != 'None') ).count() == 0:
            df_raw_data = df_raw_data.withColumn("Pack_Number", func.lit(0))
            
    # 4、ID列补位
    df_raw_data = dealIDLength(df_raw_data)
    df_cpa_pha_mapping = dealIDLength(df_cpa_pha_mapping)

    # %%
    # ========== 数据准备 =========
    # 1. 城市标准化
    
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
    df_product_map = df_product_map.dropDuplicates(["min1"])

    # %%
    # ========== 数据 mapping =========
    # 一. raw_data 基础信息匹配
    '''
    raw_data 用min1匹配 product_map的标准列
    '''
    df_cpa_pha_mapping = df_cpa_pha_mapping.where(col("推荐版本") == 1) \
                                        .select("ID", "PHA").distinct()
    
    df_universe = df_universe.withColumnRenamed('新版ID', 'PHA') \
                        .withColumnRenamed('新版名称', 'PHA医院名称')
    
    df_mkt_mapping = df_mkt_mapping.withColumnRenamed("标准通用名", "通用名") \
                                    .withColumnRenamed("mkt", "DOI")
    # min1 生成
    df_raw_data = df_raw_data.withColumn("Brand", func.when((col('Brand').isNull()) | (col('Brand') == 'NA'), col('Molecule')). \
                                             otherwise(col('Brand')))
    df_raw_data = df_raw_data.withColumn('Pack_Number', col('Pack_Number').cast(StringType()))
    df_raw_data = df_raw_data.withColumn("min1", func.concat_ws(minimum_product_sep, 
                                    *[func.when(col(i).isNull(), func.lit("NA")).otherwise(col(i)) for i in minimum_product_columns]))
    
    # 基础信息匹配
    df_raw_data = df_raw_data.join(df_cpa_pha_mapping, on="ID", how="left") \
                        .join(df_universe, on="PHA", how="left") \
                        .join(df_product_map, on='min1', how="left") \
                        .join(df_mkt_mapping, on="通用名", how="left")

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
    
    df_raw_data_standard = getStandard(df_raw_data)

    # %%
    # 全量结果汇总
    std_names = ["Date", "ID", "Raw_Hosp_Name", "Brand", "Form", "Specifications", "Pack_Number", "Manufacturer", "Molecule",
             "Source", "Sales", "Units", "Units_Box", "PHA", "PHA医院名称", "Province", "City", "min1"]
    
    if "Raw_Hosp_Name" not in df_raw_data_standard.columns:
        df_raw_data_standard = df_raw_data_standard.withColumn("Raw_Hosp_Name", func.lit("0"))
    if "Units_Box" not in df_raw_data_standard.columns:
        df_raw_data_standard = df_raw_data_standard.withColumn("Units_Box", func.lit(0))
        
    # 有的项目Raw_Hosp_Name全都为null，会在提数中间结果写出再读取时引起报错
    df_raw_data_standard = df_raw_data_standard.withColumn("Raw_Hosp_Name", func.when(col('Raw_Hosp_Name').isNull(), func.lit("0")) \
                                                                    .otherwise(col('Raw_Hosp_Name')))
    
    for each in df_raw_data_standard.columns:                                                                
        df_raw_data_standard = df_raw_data_standard.withColumn(each, col(each).cast(StringType()))
    
    # df_raw_data_standard = df_raw_data_standard.withColumn("project", func.lit(project_name))
        
    df_raw_data_standard = df_raw_data_standard.select(std_names + ["DOI", "标准通用名", "标准商品名", "标准剂型", "标准规格", 
        "标准包装数量", "标准生产企业", "标准省份名称", "标准城市名称", "PACK_ID", "ATC"])
    
    df_raw_data_standard = df_raw_data_standard.withColumn("SALES", col('SALES').cast('double')) \
                                                .withColumn("UNITS", col('UNITS').cast('double')) \
                                                .withColumn("UNITS_BOX", col('UNITS_BOX').cast('double')) \
                                                .withColumn("PACK_ID", col('PACK_ID').cast('int'))
    
    # 目录结果汇总
    df_raw_data_standard_brief = df_raw_data_standard.select("Date", "标准通用名", "ATC", "DOI", "PHA", "Source").distinct()
    
    
    # 列名转为大写
    df_raw_data_standard = df_raw_data_standard.toDF(*[i.upper() for i in df_raw_data_standard.columns])
    df_raw_data_standard_brief = df_raw_data_standard_brief.toDF(*[i.upper() for i in df_raw_data_standard_brief.columns])

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
    
    def outResultForExtractRaw(df, p_out):
        df = df.withColumn('version', func.lit(run_id)) \
                .withColumn('provider', func.lit(project_name)) \
                .withColumn('owner', func.lit(owner))
        df.repartition(1).write.format("parquet") \
                 .mode("overwrite").partitionBy("DATE") \
                 .parquet(p_out)

    # %%
    # ========== 数据输出 =========
    
    # outResult(df_raw_data_standard, p_out_raw_standard)
    # print("输出 raw_standard_out：" + p_out_raw_standard)
    
    # outResult(df_raw_data_standard_brief, p_out_raw_standard_brief)
    # print("输出 raw_standard_brief：" + p_out_raw_standard_brief)
    
    # pdf = df_raw_data_standard_brief.select('DATE').distinct().toPandas()
    # for indexs in pdf.index:
    #     data = pdf.loc[indexs]
    #     i_data = str(data['DATE'])    
    #     createPartition(p_out_raw_standard, i_data)
    #     createPartition(p_out_raw_standard_brief, i_data)
    # print('数据执行-Finish')
    
    # =========== 数据输出 =============
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    df_raw_data_standard = lowerColumns(df_raw_data_standard)
    
    logger.debug('数据执行-Finish')
    
    return {'out_df':df_raw_data_standard}
