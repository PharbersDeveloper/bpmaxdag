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
    model_month_right = kwargs['model_month_right']
    model_month_left = kwargs['model_month_left']
    ims_info_auto = kwargs['ims_info_auto']
    factor_optimize = kwargs['factor_optimize']
    ### input args ###
    
    ### output args ###
    p_out = kwargs['p_out']
    out_mode = kwargs['out_mode']
    run_id = kwargs['run_id'].replace(":","_")
    owner = kwargs['owner']
    project_name = kwargs['project_name']
    g_database_temp = kwargs['g_database_temp']
    ### output args ###

    
    
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import os
    import time
    import re
    from copy import deepcopy    
    from phcli.ph_tools.addTable.addTableToGlue import AddTableToGlue
    
    # %%
    # =========== 参数处理 =========== 
    if factor_optimize != "True":
         return {}
        
    if ims_info_auto != "False" and ims_info_auto != "True":
        logger.debug('wrong input: test, False or True') 
        raise ValueError('wrong input: test, False or True')
    
    g_table_check = 'ims_info_check'
  
    # %% 
    # =========== 输入数据读取 =========== 
    def dealToNull(df):
        df = df.replace(["None", ""], None)
        return df
    def dealScheme(df, dict_scheme):
        # 数据类型处理
        for i in dict_scheme.keys():
            df = df.withColumn(i, col(i).cast(dict_scheme[i]))
        return df
    
    if ims_info_auto == 'True':
        df_ims_mapping = kwargs['df_ims_mapping']
        df_ims_mapping = dealToNull(df_ims_mapping)
        
        df_ims_sales = kwargs['df_cn_IMS_Sales_Fdata']
        df_ims_sales = dealToNull(df_ims_sales)
        
        df_geo_map = kwargs['df_cn_geog_dimn']
        df_geo_map = dealToNull(df_geo_map)
        
    else:
        df_ims_info = kwargs['df_ims_info_upload']
        df_ims_info = dealToNull(df_ims_info)

    df_mkt_mapping = kwargs['df_mkt_mapping']
    df_mkt_mapping = dealToNull(df_mkt_mapping)   

    df_prod_mapping = kwargs['df_prod_mapping']
    df_prod_mapping = dealToNull(df_prod_mapping)
    
    
    # ============== 删除已有的s3中间文件 =============
    import boto3
    def deletePath(path_dir):
        file_name = path_dir.replace('//', '/').split('s3:/ph-platform/')[1]
        s3 = boto3.resource('s3', region_name='cn-northwest-1',
                            aws_access_key_id="AKIAWPBDTVEAEU44ZAGT",
                            aws_secret_access_key="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")
        bucket = s3.Bucket('ph-platform')
        bucket.objects.filter(Prefix=file_name).delete()
    deletePath(path_dir=f"{p_out + g_table_check}/version={run_id}/provider={project_name}/owner={owner}/")
    
    
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
    
    # 1、prod_mapping 清洗
    dict_cols_prod_map = {"通用名":["通用名", "标准通用名", "通用名_标准", "药品名称_标准", "S_Molecule_Name"], 
                          "min2":["min2", "min1_标准"],
                          "pfc":["pfc", "packcode", "Pack_ID", "PackID", "packid"],
                          "标准商品名":["标准商品名", "商品名_标准", "S_Product_Name"],
                          "标准剂型":["标准剂型", "剂型_标准", "Form_std", "S_Dosage"],
                          "标准规格":["标准规格", "规格_标准", "Specifications_std", "药品规格_标准", "S_Pack"],
                          "标准包装数量":["标准包装数量", "包装数量2", "包装数量_标准", "Pack_Number_std", "S_PackNumber", "最小包装数量"],
                          "标准生产企业":["标准生产企业", "标准企业", "生产企业_标准", "Manufacturer_std", "S_CORPORATION", "标准生产厂家"]
                         }
    
    df_prod_mapping = getTrueColRenamed(df_prod_mapping, dict_cols_prod_map, df_prod_mapping.columns)
    df_prod_mapping = dealScheme(df_prod_mapping, {"标准包装数量":"int", "pfc":"int"})
    
    df_prod_mapping = df_prod_mapping.withColumn("min2", func.regexp_replace("min2", "&amp;", "&")) \
                                    .withColumn("min2", func.regexp_replace("min2", "&lt;", "<")) \
                                    .withColumn("min2", func.regexp_replace("min2", "&gt;", ">"))
    df_prod_mapping = df_prod_mapping.select('通用名', '标准商品名', '标准剂型', '标准规格', '标准包装数量',
                                             
                                        '标准生产企业', 'min2', 'pfc') \
                                    .distinct() \
                                    .withColumnRenamed('通用名', 'Molecule') \
                                    .withColumnRenamed('标准商品名', 'Brand') \
                                    .withColumnRenamed('标准剂型', 'Form') \
                                    .withColumnRenamed('标准规格', 'Specifications') \
                                    .withColumnRenamed('标准包装数量', 'Pack_Number') \
                                    .withColumnRenamed('标准生产企业', 'Manufacturer') \
                                    .withColumnRenamed('pfc', 'Pack_ID')
        
    # 2. df_mkt_mapping
    df_mkt_mapping = df_mkt_mapping.withColumnRenamed("标准通用名", "通用名") \
                                    .withColumnRenamed("model", "mkt") \
                                    .select("mkt", "通用名").distinct()
    
        
    df_model_pfc = df_prod_mapping.where(~col('Pack_ID').isNull()).select('Molecule', 'Pack_ID').distinct() \
                        .join(df_mkt_mapping, df_prod_mapping['Molecule']==df_mkt_mapping['通用名'], how='left')
    df_model_pfc = df_model_pfc.select('Pack_ID', 'mkt').distinct()
    
    # %%
    # =========== 数据执行 ============    
    def lowerColumns(df):
            df = df.toDF(*[i.lower() for i in df.columns])
            return df
        
    @udf(StringType())
    def city_change(name):
        # 城市名定义
        if name in ["苏锡城市群"]:
            newname = "苏锡市"
        elif name in ["全国"]:
            newname = "CHPA"
        else:
            newname = name + '市'
        return newname
        
    # %%
    # 4. ims文件
    if ims_info_auto == 'True':   
        # geo_map_path 匹配城市中文名
        df_geo_map = df_geo_map.select('GEO_CD', 'GEO_DESC_CN').distinct() \
                        .withColumnRenamed('GEO_CD', 'Geography_id')
        
        # ims_mapping 匹配产品英文信息
        df_ims_mapping = df_ims_mapping.select('Molecule_Composition', 'Prd_desc', 'Pack_Id0').distinct() \
                                .withColumnRenamed('Pack_Id0', 'PACK_ID')
        # ims 销量数据
        df_ims_sales = df_ims_sales.select('Geography_id', 'Pack_ID', 'Period_Code', 'LC')
    
        # 是否补充ims 销量数据（增加输入文件的version选择）
        # if add_imsinfo_path != 'Empty':
        #     add_imsinfo_file = spark.read.csv(add_imsinfo_path, header=True)
        #     add_imsinfo_file = add_imsinfo_file.select('Geography_id', 'Pack_ID', 'Period_Code', 'LC')
        #     # 去掉add_imsinfo_file中有的
        #     df_ims_sales_keep = df_ims_sales.join(add_imsinfo_file, on=["Pack_ID", "Geography_id"], how='left_anti')
        #     df_ims_sales = df_ims_sales_keep.union(add_imsinfo_file.select(df_ims_sales_keep.columns))
    
        # 信息匹配
        df_ims_sales = df_ims_sales.join(df_ims_mapping, on='Pack_ID', how='left') \
                            .join(df_model_pfc, on='Pack_ID', how='left') \
                            .join(df_geo_map, on='Geography_id', how='left')
    
        df_ims_sales = df_ims_sales.withColumn('Date', func.regexp_replace('Period_Code', 'M', '')) \
                            .withColumn('City', city_change(col('GEO_DESC_CN'))) \
                            .where(col('Date').between(model_month_left, model_month_right))
    
        # 检查文件同一个分子是否有没匹配上的packid
        df_check = df_ims_sales.where(col('City') == 'CHPA') \
                        .groupby('Molecule_Composition', 'mkt').agg(func.sum('LC').alias('Sales'))
        df_check_mol = df_check.groupby('Molecule_Composition').agg(func.sum('Sales').alias('Sales_mol'))
        df_check = df_check.join(df_check_mol, on='Molecule_Composition', how='left')
        df_check = df_check.withColumn('share', col('Sales')/col('Sales_mol'))
        df_molecules_in_model = df_check.where(~col('mkt').isNull()).select('Molecule_Composition').distinct()
        df_check = df_check.join(df_molecules_in_model, on='Molecule_Composition', how='inner')
    
        # 根据分子名重新匹配市场名，补充上缺失的数据
        df_ims_sales_all = df_ims_sales.drop('mkt') \
                                        .join(df_check.where(~col('mkt').isNull()).select("Molecule_Composition","mkt").distinct(), 
                                              on='Molecule_Composition', how='left')
        # 计算share
        df_ims_sales_all = df_ims_sales_all.groupby('City', 'Pack_ID', 'Prd_desc', 'Molecule_Composition', 'mkt') \
                                    .agg(func.sum('LC').alias('ims_poi_vol'))
        
        df_ims_sales_tmp = df_ims_sales_all.groupby('City', 'mkt') \
                                    .agg(func.sum('ims_poi_vol').alias('ims_poi_vol_all'))
        
        df_ims_sales_all = df_ims_sales_all.join(df_ims_sales_tmp, on=['City', 'mkt'], how='left') \
                                    .withColumn('ims_share', col('ims_poi_vol')/col('ims_poi_vol_all'))
        
        # 整理
        df_ims_sales_all = df_ims_sales_all.drop("ims_poi_vol_all") \
                                    .withColumnRenamed("mkt", "model") \
                                    .where(~col('model').isNull())
    
        # 输出检查文件
        df_check = lowerColumns(df_check)
        AddTableToGlue(df=df_check, database_name_of_output=g_database_temp, table_name_of_output=g_table_check, 
                           path_of_output_file=p_out, mode=out_mode) \
                    .add_info_of_partitionby({"version":run_id,"provider":project_name,"owner":owner})
        
        # 输出 ims_sales_all
        return {'out_df': df_ims_sales_all}
    else:
        return {'out_df': df_ims_info}

