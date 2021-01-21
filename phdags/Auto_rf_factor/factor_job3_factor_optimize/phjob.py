# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
import os
import time
import re
from copy import deepcopy

def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"])
    logger.info("当前 owner 为 " + str(kwargs["owner"]))
    logger.info("当前 run_id 为 " + str(kwargs["run_id"]))
    logger.info("当前 job_id 为 " + str(kwargs["job_id"]))
    spark = kwargs["spark"]()
    #logger.info(kwargs["a"])
    #logger.info(kwargs["b"])
    #logger.info(kwargs["c"])
    #logger.info(kwargs["d"])
    #return {}
    '''
    os.environ["PYSPARK_PYTHON"] = "python3"
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data from s3") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "1") \
        .config("spark.executor.memory", "1g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .getOrCreate()
    
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if access_key is not None:
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")
    
    max_path = 's3a://ph-max-auto/v0.0.1-2020-06-08/'
    project_name = 'Eisai'
    outdir = '202009'
    model_month_right = '201912'
    model_month_left = '201901'
    all_models = '固力康'
    max_file = 'MAX_result_201701_202009_city_level'
    '''
    # 输入 
    max_path = kwargs["max_path"]
    project_name = kwargs["project_name"]
    max_file = kwargs["max_file"]
    outdir = kwargs["outdir"]
    model_month_right = kwargs["model_month_right"]
    model_month_left = kwargs["model_month_left"]
    all_models = kwargs["all_models"]
    test = kwargs["test"]
    
    if test != "False" and test != "True":
        phlogger.error('wrong input: test, False or True') 
        raise ValueError('wrong input: test, False or True')
    
    
    model_month_right = int(model_month_right)
    model_month_left = int(model_month_left)
    all_models = all_models.replace(' ','').split(',')
    
    max_result_path = max_path + '/' + project_name + '/' + outdir + '/MAX_result/' + max_file
    product_map_path = max_path + '/' + project_name + '/' + outdir + '/prod_mapping'
    
    # =========== 数据执行 ============
    logger.info("job3_factor_optimize")
    # 1. max 文件处理
    max_result = spark.read.parquet(max_result_path)
    max_result = max_result.where((col('Date') >= model_month_left) & (col('Date') <= model_month_right))
    
    # 2. product_map 文件处理
    product_map = spark.read.parquet(product_map_path)
    if project_name == "Sanofi" or project_name == "AZ":
        product_map = product_map.withColumnRenamed(product_map.columns[21], "pfc")
    if project_name == "Eisai":
        product_map = product_map.withColumnRenamed(product_map.columns[22], "pfc")
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
                            .withColumn('pfc', product_map.pfc.cast(DoubleType())) \
                            .withColumnRenamed('pfc', 'Pack_ID')
    
    
    # 3. 对每个市场优化factor
    # market = '固力康'
    for market in all_models:
        logger.info("当前market为:" + str(market))
        # 输入
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
        ims_info = spark.read.parquet(ims_info_path)
        ims_info = ims_info.withColumnRenamed('city', 'City')
        
        factor = spark.read.parquet(factor_path)
        
        # 3.1 max 数据
        max_df = max_result.where(col('DOI') == market)
        max_df = max_df.join(factor, on='City', how='left')
        max_df = max_df.withColumn('Predict_Sales', func.when(col('PANEL') == 0, col('factor')*col('Predict_Sales')) \
                                                        .otherwise(col('Predict_Sales'))) \
                        .withColumn('Citynew', col('City'))
        
        max_df = max_df.withColumn('Citynew', func.when(col('City').isin("福州市","厦门市","泉州市"), func.lit('福厦泉市')) \
                                    .otherwise(func.when(col('City').isin("佛山市","中山市","东莞市","珠海市"), func.lit('珠三角市')) \
                                                    .otherwise(col('Citynew')))
                                    )
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
        logger.info("cvxpy优化")
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
            
        value_other = factor2.where(col('City')=='other').select('factor2').toPandas()['factor2'][0]    
        factor3 = factor3.withColumn('factor2', func.when(col('factor2').isNull(), func.lit(value_other)) \
                                                    .otherwise(col('factor2')))
        
        factor3 = factor3.withColumn('factor', col('factor1') * col('factor2'))
        factor3 = factor3.withColumn('factor', func.when(col('factor') > 4, func.lit(4)).otherwise(col('factor')))
        
        factor3 = factor3.repartition(1)
        factor3.write.format("parquet") \
                .mode("overwrite").save(factor_out_path)
        logger.info("finish:" + str(market))
                
    return {}