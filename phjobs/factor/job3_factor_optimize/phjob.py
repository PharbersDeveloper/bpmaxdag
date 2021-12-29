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
    max_path = kwargs['max_path']
    project_name = kwargs['project_name']
    outdir = kwargs['outdir']
    model_month_right = kwargs['model_month_right']
    model_month_left = kwargs['model_month_left']
    all_models = kwargs['all_models']
    max_file = kwargs['max_file']
    test = kwargs['test']
    ims_info_auto = kwargs['ims_info_auto']
    ims_version = kwargs['ims_version']
    add_imsinfo_path = kwargs['add_imsinfo_path']
    geo_map_path = kwargs['geo_map_path']
    factor_optimize = kwargs['factor_optimize']
    ### input args ###
    
    ### output args ###
    c = kwargs['c']
    d = kwargs['d']
    ### output args ###

    
    
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import os
    import time
    import re
    from copy import deepcopy    
    # %%
    '''
    project_name = 'Takeda'
    outdir = '202012'
    model_month_right = '202012'
    model_month_left = '202001'
    all_models = 'TK1'
    max_file = 'MAX_result_201801_202012_city_level'
    test = 'True'
    ims_version = '202012'
    add_imsinfo_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/Takeda/add_ims_info.csv'
    geo_map_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/Takeda/geo_map.csv'
    
    project_name = '神州'
    outdir = '201912'
    model_month_right = '201912'
    model_month_left = '201901'
    all_models = 'SZ1'
    max_file = 'MAX_result_201801_201912_city_level'
    test = 'True'
    ims_version = '202010'
    '''
    # %%
    # 输入
    if factor_optimize != "True":
         raise ValueError('不进行优化')
            
    if test != "False" and test != "True":
        logger.debug('wrong input: test, False or True') 
        raise ValueError('wrong input: test, False or True')
    if ims_info_auto != "False" and ims_info_auto != "True":
        logger.debug('wrong input: test, False or True') 
        raise ValueError('wrong input: test, False or True')
    
    model_month_right = int(model_month_right)
    model_month_left = int(model_month_left)
    all_models = all_models.replace(' ','').split(',')
    
    max_result_path = max_path + '/' + project_name + '/' + outdir + '/MAX_result/' + max_file
    product_map_path = max_path + '/' + project_name + '/' + outdir + '/prod_mapping'
    mkt_map_path = max_path + '/' + project_name + '/mkt_mapping'
    
    if ims_info_auto == 'True':
        ims_mapping_path = max_path + '/' + '/Common_files/IMS_flat_files/' + ims_version + '/ims_mapping_' + ims_version + '.csv'
        ims_sales_path = max_path + '/Common_files/IMS_flat_files/' + ims_version + '/cn_IMS_Sales_Fdata_' + ims_version + '_1.csv'
        if geo_map_path == 'Empty':
            geo_map_path = max_path + '/' + '/Common_files/IMS_flat_files/' + ims_version + '/cn_geog_dimn_' + ims_version + '_1.csv'
        # 输出检查文件
        check_path = max_path + '/' + project_name + '/ims_info/ims_info_check'
        ims_sales_all_path = max_path + '/' + project_name + '/ims_info/ims_info_all'
            
    # 输出在每个循环下

    # %%
    # =========== 数据执行 ============
    logger.debug("job3_factor_optimize")
    # 1. max 文件处理
    max_result = spark.read.parquet(max_result_path)
    max_result = max_result.where((col('Date') >= model_month_left) & (col('Date') <= model_month_right))
    
    # 2. product_map 文件处理
    product_map = spark.read.parquet(product_map_path)
    #if project_name == "Sanofi" or project_name == "AZ":
    #    if "pfc" not in product_map.columns:
    #        product_map = product_map.withColumnRenamed([i for i in product_map.columns if 'pfc' in i][0], "pfc")
    #if project_name == "Eisai":
    #    if "pfc" not in product_map.columns:
    #        product_map = product_map.withColumnRenamed([i for i in product_map.columns if 'pfc' in i][0], "pfc")
    for i in product_map.columns:
        if i in ["标准通用名", "通用名_标准", "药品名称_标准", "S_Molecule_Name"]:
            product_map = product_map.withColumnRenamed(i, "通用名")
        if i in ["min1_标准"]:
            product_map = product_map.withColumnRenamed(i, "min2")
        if i in ["packcode", "Pack_ID", "Pack_Id", "PackID", "packid"]:
            product_map = product_map.withColumnRenamed(i, "pfc")
        if i in ["商品名_标准", "S_Product_Name"]:
            product_map = product_map.withColumnRenamed(i, "标准商品名")
        if i in ["剂型_标准", "Form_std", "S_Dosage"]:
            product_map = product_map.withColumnRenamed(i, "标准剂型")
        if i in ["规格_标准", "Specifications_std", "药品规格_标准", "S_Pack"]:
            product_map = product_map.withColumnRenamed(i, "标准规格")
        if i in ["包装数量2", "包装数量_标准", "Pack_Number_std", "S_PackNumber", "最小包装数量"]:
            product_map = product_map.withColumnRenamed(i, "标准包装数量")
        if i in ["标准企业", "生产企业_标准", "Manufacturer_std", "S_CORPORATION", "标准生产厂家"]:
            product_map = product_map.withColumnRenamed(i, "标准生产企业")
    if project_name == "Janssen" or project_name == "NHWA":
        if "标准剂型" not in product_map.columns:
            product_map = product_map.withColumnRenamed("剂型", "标准剂型")
        if "标准规格" not in product_map.columns:
            product_map = product_map.withColumnRenamed("规格", "标准规格")
        if "标准生产企业" not in product_map.columns:
            product_map = product_map.withColumnRenamed("生产企业", "标准生产企业")
        if "标准包装数量" not in product_map.columns:
            product_map = product_map.withColumnRenamed("包装数量", "标准包装数量")
    product_map = product_map.withColumn("min2", func.regexp_replace("min2", "&amp;", "&")) \
                            .withColumn("min2", func.regexp_replace("min2", "&lt;", "<")) \
                            .withColumn("min2", func.regexp_replace("min2", "&gt;", ">"))
    
    product_map = product_map.select('通用名', '标准商品名', '标准剂型', '标准规格', '标准包装数量', 
                                        '标准生产企业', 'min2', 'pfc') \
                            .distinct() \
                            .withColumnRenamed('通用名', 'Molecule') \
                            .withColumnRenamed('标准商品名', 'Brand') \
                            .withColumnRenamed('标准剂型', 'Form') \
                            .withColumnRenamed('标准规格', 'Specifications') \
                            .withColumnRenamed('标准包装数量', 'Pack_Number') \
                            .withColumnRenamed('标准生产企业', 'Manufacturer') \
                            .withColumn('pfc', product_map.pfc.cast(IntegerType())) \
                            .withColumnRenamed('pfc', 'Pack_ID')

    # %%
    # 3. mkt_map_path
    mkt_map = spark.read.parquet(mkt_map_path)
    mkt_map = mkt_map.withColumnRenamed("标准通用名", "通用名") \
        .withColumnRenamed("model", "mkt") \
        .select("mkt", "通用名").distinct()
    model_pfc = product_map.where(~col('Pack_ID').isNull()).select('Molecule', 'Pack_ID').distinct() \
                        .join(mkt_map, product_map['Molecule']==mkt_map['通用名'], how='left')
    model_pfc = model_pfc.select('Pack_ID', 'mkt').distinct()

    # %%
    # 4. ims文件
    if ims_info_auto == 'True':
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
    
        # geo_map_path 匹配城市中文名
        geo_map = spark.read.csv(geo_map_path, header=True)
        geo_map = geo_map.select('GEO_CD', 'GEO_DESC_CN').distinct() \
                        .withColumnRenamed('GEO_CD', 'Geography_id')
        # ims_mapping 匹配产品英文信息
        ims_mapping = spark.read.csv(ims_mapping_path, header=True)
        ims_mapping = ims_mapping.select('Molecule_Composition', 'Prd_desc', 'Pack_Id0').distinct() \
                                .withColumnRenamed('Pack_Id0', 'PACK_ID')
        # ims 销量数据
        ims_sales = spark.read.csv(ims_sales_path, header=True)
        ims_sales = ims_sales.select('Geography_id', 'Pack_ID', 'Period_Code', 'LC')
    
        # 是否补充ims 销量数据
        if add_imsinfo_path != 'Empty':
            add_imsinfo_file = spark.read.csv(add_imsinfo_path, header=True)
            add_imsinfo_file = add_imsinfo_file.select('Geography_id', 'Pack_ID', 'Period_Code', 'LC')
            # 去掉add_imsinfo_file中有的
            ims_sales_keep = ims_sales.join(add_imsinfo_file, on=["Pack_ID", "Geography_id"], how='left_anti')
            ims_sales = ims_sales_keep.union(add_imsinfo_file.select(ims_sales_keep.columns))
    
        # 信息匹配
        ims_sales = ims_sales.join(ims_mapping, on='Pack_ID', how='left') \
                            .join(model_pfc, on='Pack_ID', how='left') \
                            .join(geo_map, on='Geography_id', how='left')
    
        ims_sales = ims_sales.withColumn('Date', func.regexp_replace('Period_Code', 'M', '')) \
                            .withColumn('City', city_change(col('GEO_DESC_CN'))) \
                            .where(col('Date').between(model_month_left, model_month_right))
    
        # 检查文件同一个分子是否有没匹配上的packid
        check = ims_sales.where(col('City') == 'CHPA') \
                        .groupby('Molecule_Composition', 'mkt').agg(func.sum('LC').alias('Sales'))
        check_mol = check.groupby('Molecule_Composition').agg(func.sum('Sales').alias('Sales_mol'))
        check = check.join(check_mol, on='Molecule_Composition', how='left')
        check = check.withColumn('share', col('Sales')/col('Sales_mol'))
        molecules_in_model = check.where(~col('mkt').isNull()).select('Molecule_Composition').distinct()
        check = check.join(molecules_in_model, on='Molecule_Composition', how='inner')
    
        # 根据分子名重新匹配市场名，补充上缺失的数据
        ims_sales_all = ims_sales.drop('mkt') \
                                .join(check.where(~col('mkt').isNull()).select("Molecule_Composition","mkt").distinct(), 
                                      on='Molecule_Composition', how='left')
        # 计算share
        ims_sales_all = ims_sales_all.groupby('City', 'Pack_ID', 'Prd_desc', 'Molecule_Composition', 'mkt') \
                                    .agg(func.sum('LC').alias('ims_poi_vol'))
        ims_sales_tmp = ims_sales_all.groupby('City', 'mkt') \
                                    .agg(func.sum('ims_poi_vol').alias('ims_poi_vol_all'))
        ims_sales_all = ims_sales_all.join(ims_sales_tmp, on=['City', 'mkt'], how='left') \
                                    .withColumn('ims_share', col('ims_poi_vol')/col('ims_poi_vol_all'))
        # 整理
        ims_sales_all = ims_sales_all.drop("ims_poi_vol_all") \
                                    .withColumnRenamed("mkt", "model") \
                                    .where(~col('model').isNull())
    
        # 输出检查文件
        check = check.repartition(1)
        check.write.format("parquet") \
                .mode("overwrite").save(check_path)
        
        # 输出 ims_sales_all
        ims_sales_all = ims_sales_all.repartition(2)
        ims_sales_all.write.format("parquet") \
                .mode("overwrite").save(ims_sales_all_path)
        
        ims_sales_all = spark.read.parquet(ims_sales_all_path)

    # %%
    # 3. 对每个市场优化factor
    @udf(StringType())
    def city_change(name):
        # 城市名定义
        if name in ["福州市", "厦门市", "泉州市"]:
            newname = "福厦泉市"
        elif name in ["珠海市", "东莞市", "中山市", "佛山市"]:
            newname = "珠三角市"
        elif name in ["绍兴市", "嘉兴市", "台州市", "金华市"]:
            newname = "浙江市"
        elif name in ["苏州市", "无锡市"]:
            newname = "苏锡市"
        else:
            newname = name
        return newname
    
    def unpivot(df, keys):
        # 功能：数据宽变长
        # 参数说明 df:dataframe,  keys 待转换表中需要保留的主键key，以list[]类型传入
        # 转换是为了避免字段类不匹配，统一将数据转换为string类型，如果保证数据类型完全一致，可以省略该句
        df = df.select(*[col(_).astype("string") for _ in df.columns])
        cols = [_ for _ in df.columns if _ not in keys]
        stack_str = ','.join(map(lambda x: "'%s', `%s`" % (x, x), cols))
        # feature, value 转换后的列名，可自定义
        df = df.selectExpr(*keys, "stack(%s, %s) as (feature, value)" % (len(cols), stack_str))
        return df
    
    # market = '固力康'
    for market in all_models:
        logger.debug("当前market为:" + str(market))
        # 输入
        if ims_info_auto != 'True':     
            ims_info_path = max_path + '/' + project_name + '/ims_info/' +  market + '_ims_info'
        factor_path = max_path + '/' + project_name + '/forest/' + market + '_factor_1'
        # 输出
        ims_v1_otherall_path = max_path + '/' + project_name + '/forest/' + market + '_top3_product.csv'
        ims_panel_max_out_path = max_path + '/' + project_name + '/forest/' + market + '_factor_gap.csv'
        if test == 'True':
            factor_out_path = max_path + '/' + project_name + '/forest/factor/factor_' + market
        else:
            factor_out_path = max_path + '/' + project_name + '/factor/factor_' + market
    
        # 数据执行
        if ims_info_auto != 'True':
            ims_info = spark.read.parquet(ims_info_path)
            ims_info = ims_info.withColumnRenamed('city', 'City')
        else:
            ims_info = ims_sales_all.where(col('model') == market)
    
        factor = spark.read.parquet(factor_path)
    
        # 3.1 max 数据
        max_df = max_result.where(col('DOI') == market)
        max_df = max_df.join(factor, on='City', how='left')
        max_df = max_df.withColumn('Predict_Sales', func.when(col('PANEL') == 0, col('factor')*col('Predict_Sales')) \
                                                        .otherwise(col('Predict_Sales'))) \
                        .withColumn('Citynew', col('City'))
    
        max_df = max_df.withColumn('Citynew', city_change(col('City')))
        max_df = max_df.withColumn('Citynew', func.when(~col('Citynew').isin(ims_info.select('City').distinct().toPandas()['City'].values.tolist()), 
                                                        func.lit('other')).otherwise(col('Citynew')))
    
        max_df = max_df.join(product_map.dropDuplicates(['min2']), max_df.Prod_Name==product_map.min2, how='left')
    
        # 城市产品层面
        max2 = max_df.groupBy('Brand', 'Citynew').agg(func.sum('Predict_Sales').alias('max_Prod')) \
                    .withColumn('mkt', func.lit(market)) \
                    .withColumnRenamed('Citynew', 'City')
        # 全国的市场
        max3 = max_df.groupBy('Citynew').agg(func.sum('Predict_Sales').alias('max_mkt')) \
                    .withColumn('Market', func.lit(market)) \
                    .withColumnRenamed('Citynew', 'City')
    
        # 3.2 panel数据
        bll1 = max_df.where(max_df.PANEL == 1)
        # 城市产品层面
        bll2 = bll1.groupBy('Brand', 'Citynew').agg(func.sum('Predict_Sales').alias('panel_Sales')) \
                    .withColumn('mkt', func.lit(market)) \
                    .withColumnRenamed('Citynew', 'City')
        # 全国的市场
        bll3 = bll1.groupBy('Citynew').agg(func.sum('Predict_Sales').alias('panel_mkt')) \
                    .withColumn('Market', func.lit(market)) \
                    .withColumnRenamed('Citynew', 'City')
    
        # 3.3 确定IMS三个最大的产品
        ims_df = ims_info.withColumn('Market', func.lit(market))
    
        ims_df = ims_df.withColumn('Brand_en', col('Prd_desc')) \
                            .join(product_map.select('Pack_ID', 'Brand').dropDuplicates(['Pack_ID']), on='Pack_ID', how='left')
    
        ims_df = ims_df.groupBy('Market', 'Brand', 'City').agg(func.sum('ims_poi_vol').alias('Prod')) \
                        .withColumn('Brand', func.when(col('Brand').isNull(), func.lit('none')).otherwise(col('Brand')))
    
        # 全国的商品销量
        ims_part1 = ims_df.where(col('City') == 'CHPA')
        ims_part1_v1 = ims_part1.groupBy('Market', 'Brand').agg(func.sum('Prod').alias('Prod_CHPA'))
        # 非other 省份的商品销量
        ims_part2 = ims_df.where(col('City') != 'CHPA')
        ims_part2_v1 = ims_part2.groupBy('Market', 'Brand').agg(func.sum('Prod').alias('Prod_nonCHPA'))
        # other 省份的商品销量
        ims_v1_left = ims_part1_v1.join(ims_part2_v1, on=['Market','Brand'], how='left')
        ims_v1_left = ims_v1_left.fillna(0, 'Prod_nonCHPA') \
                                .withColumn('Prod_other', col('Prod_CHPA')-col('Prod_nonCHPA')) \
                                .withColumn('City', func.lit('other'))
        ims_v1_left = ims_v1_left.withColumnRenamed('Prod_CHPA', 'CHPA') \
                                .withColumnRenamed('Prod_nonCHPA', 'IMS_City') \
                                .withColumnRenamed('Prod_other', 'Prod') \
                                .select('Market','Brand','City','Prod')
        # other + 非other省份 每个城市的销量前三名产品
        ims_v1_otherall = ims_part2.union(ims_v1_left)
        ims_v1_otherall = ims_v1_otherall.withColumn('n',
                                func.row_number().over(Window.partitionBy('Market', 'City').orderBy(col('Prod').desc())))
        ims_v1_otherall = ims_v1_otherall.where(col('n') < 4) \
                                        .orderBy(col('Market'), col('City'), col('Prod').desc()) \
                                        .withColumnRenamed('Prod', 'ims_Prod').persist()
    
        ims_v1_mkt =  ims_part2.union(ims_v1_left) \
                                .groupBy('Market','City').agg(func.sum('Prod').alias('ims_mkt'))
    
        ims_panel_max = ims_v1_otherall.join(max2.withColumnRenamed('mkt', 'Market'), 
                                            on=['Market', 'Brand', 'City'], how='left') \
                                        .join(bll2.withColumnRenamed('mkt', 'Market'),
                                            on=['Market', 'Brand', 'City'], how='left')
        ims_panel_max = unpivot(ims_panel_max, ['Market', 'Brand', 'City', 'n'])
        ims_panel_max = ims_panel_max.withColumn('value', col('value').cast(DoubleType())) \
                                    .withColumn('new', func.concat(col('feature'), func.lit('_'), col('n'))) \
                                    .drop('Brand', 'feature', 'n')
        ims_panel_max = ims_panel_max.groupBy('Market', 'City').pivot('new').agg(func.sum('value')).fillna(0).persist()
        ims_panel_max = ims_panel_max.join(ims_v1_mkt, on=['Market','City'], how='left') \
                                    .join(max3, on=['Market','City'], how='left') \
                                    .join(bll3, on=['Market','City'], how='left')
        ims_panel_max = ims_panel_max.fillna(0, ims_panel_max.columns[2:])
    
        if 'ims_Prod_2' not in ims_panel_max.columns:
            ims_panel_max = ims_panel_max.withColumn('ims_Prod_2', func.lit(0))
        if 'max_Prod_2' not in ims_panel_max.columns:
            ims_panel_max = ims_panel_max.withColumn('max_Prod_2', func.lit(0))
        if 'panel_Sales_2' not in ims_panel_max.columns:
            ims_panel_max = ims_panel_max.withColumn('panel_Sales_2', func.lit(0))
        if 'ims_Prod_3' not in ims_panel_max.columns:
            ims_panel_max = ims_panel_max.withColumn('ims_Prod_3', func.lit(0))
        if 'max_Prod_3' not in ims_panel_max.columns:
            ims_panel_max = ims_panel_max.withColumn('max_Prod_3', func.lit(0))
        if 'panel_Sales_3' not in ims_panel_max.columns:
            ims_panel_max = ims_panel_max.withColumn('panel_Sales_3', func.lit(0))
    
        # 3.4 优化 
        logger.debug("cvxpy优化")
        schema = deepcopy(ims_panel_max.schema)
        schema.add("factor", DoubleType())
        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
        def cvxpy_func(pdf):
            import numpy as np
            import cvxpy as cp
    
            f = cp.Variable()
            prob = cp.Problem(cp.Minimize(cp.maximum(cp.abs((f*(pdf['max_Prod_1'][0]-
                                              pdf['panel_Sales_1'][0])+
                                           pdf['panel_Sales_1'][0])/pdf['ims_Prod_1'][0]-1),
                                    cp.abs((f*(pdf['max_Prod_2'][0]-
                                              pdf['panel_Sales_2'][0])+
                                           pdf['panel_Sales_2'][0])/pdf['ims_Prod_2'][0]-1),
                                    cp.abs((f*(pdf['max_Prod_3'][0]-
                                              pdf['panel_Sales_3'][0])+
                                           pdf['panel_Sales_3'][0])/pdf['ims_Prod_3'][0]-1),
                                    cp.abs((f*(pdf['max_mkt'][0]-
                                              pdf['panel_mkt'][0])+
                                           pdf['panel_mkt'][0])/pdf['ims_mkt'][0]-1))),
                       [cp.abs((f*(pdf['max_mkt'][0]-pdf['panel_mkt'][0])+
                                   pdf['panel_mkt'][0])/pdf['ims_mkt'][0]-1) <= 0.05])
            prob.solve(solver = cp.ECOS)
    
            return pdf.assign(factor = f.value)
    
        ims_panel_max_out = ims_panel_max.groupby('Market','City').apply(cvxpy_func)
        ims_panel_max_out = ims_panel_max_out.withColumn('factor', 
                                                func.when((col('factor').isNull()) | (col('factor') < 0 ), func.lit(0)) \
                                                    .otherwise(col('factor')))
    
        ims_panel_max_out = ims_panel_max_out.withColumn('gap1', 
                (col('factor')*(col('max_Prod_1') - col('panel_Sales_1')) + col('panel_Sales_1')) / col('ims_Prod_1') -1)
        ims_panel_max_out = ims_panel_max_out.withColumn('gap2', 
                (col('factor')*(col('max_Prod_2') - col('panel_Sales_2')) + col('panel_Sales_2')) / col('ims_Prod_2') -1)
        ims_panel_max_out = ims_panel_max_out.withColumn('gap3', 
                (col('factor')*(col('max_Prod_3') - col('panel_Sales_3')) + col('panel_Sales_3')) / col('ims_Prod_3') -1)
        ims_panel_max_out = ims_panel_max_out.withColumn('gap_mkt', 
                (col('factor')*(col('max_mkt') - col('panel_mkt')) + col('panel_mkt')) / col('ims_mkt') -1)
    
        # 输出
        
        ims_v1_otherall = ims_v1_otherall.repartition(1)
        ims_v1_otherall.write.format("csv").option("header", "true") \
                .mode("overwrite").save(ims_v1_otherall_path)
    
        ims_panel_max_out = ims_panel_max_out.repartition(1)
        ims_panel_max_out.write.format("csv").option("header", "true") \
                .mode("overwrite").save(ims_panel_max_out_path)
        
        factor1 = spark.read.parquet(factor_path)
        factor1 = factor1.withColumnRenamed('factor', 'factor1')
        factor2 = ims_panel_max_out.select('City','factor').withColumnRenamed('factor', 'factor2')
        factor3 = factor1.join(factor2, on='City', how='left')
    
        factor2_city = factor2.select('City').distinct().toPandas()['City'].values.tolist()
    
        if "福厦泉市" in factor2_city:
            value = factor2.where(col('City')=='福厦泉市').select('factor2').toPandas()['factor2'][0]
            factor3 = factor3.withColumn('factor2', func.when(col('City').isin("福州市","厦门市","泉州市"), func.lit(value)) \
                                                        .otherwise(col('factor2')))
    
        if "珠三角市" in factor2_city:
            value2 = factor2.where(col('City')=='珠三角市').select('factor2').toPandas()['factor2'][0]
            factor3 = factor3.withColumn('factor2', func.when(col('City').isin("珠海市","东莞市","中山市","佛山市"), 
                                                        func.lit(value2)).otherwise(col('factor2')))
    
        if "浙江市" in factor2_city:
            value3 = factor2.where(col('City')=='浙江市').select('factor2').toPandas()['factor2'][0]
            factor3 = factor3.withColumn('factor2', func.when(col('City').isin("绍兴市","嘉兴市","台州市","金华市"), 
                                                        func.lit(value3)).otherwise(col('factor2')))
    
        if "苏锡市" in factor2_city:
            value4 = factor2.where(col('City')=='苏锡市').select('factor2').toPandas()['factor2'][0]
            factor3 = factor3.withColumn('factor2', func.when(col('City').isin("苏州市","无锡市"), 
                                                        func.lit(value4)).otherwise(col('factor2')))
    
        value_other = factor2.where(col('City')=='other').select('factor2').toPandas()['factor2'][0]    
        factor3 = factor3.withColumn('factor2', func.when(col('factor2').isNull(), func.lit(value_other)) \
                                                    .otherwise(col('factor2')))
    
        factor3 = factor3.withColumn('factor', col('factor1') * col('factor2'))
        factor3 = factor3.withColumn('factor', func.when(col('factor') > 4, func.lit(4)).otherwise(col('factor')))
        
        factor3 = factor3.repartition(1)
        factor3.write.format("parquet") \
                .mode("overwrite").save(factor_out_path)
        
        logger.debug("finish:" + str(market))

