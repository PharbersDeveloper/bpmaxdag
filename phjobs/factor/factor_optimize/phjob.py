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
    all_models = kwargs['all_models']
    factor_optimize = kwargs['factor_optimize']
    g_input_version = kwargs['g_input_version']
    ### input args ###
    
    ### output args ###
    run_id = kwargs['run_id'].replace(":","_")
    owner = kwargs['owner']
    project_name = kwargs['project_name']
    
    g_database_temp = "phdatatemp"
    p_out = "s3://ph-platform/2020-11-11/etl/temporary_files/"
    out_mode = "append"
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
    model_month_right = int(model_month_right)
    model_month_left = int(model_month_left)
    all_models = all_models.replace(' ','').split(',')
    
    g_table_ims_v1_otherall = 'factor_top3_product'
    g_table_ims_panel_max_out = 'factor_gap'
    g_table_factor3 = 'factor'
    
    # %% 
    # =========== 输入数据读取 =========== 
    def dealToNull(df):
        df = df.replace(["None", ""], None)
        return df
    
    def dealScheme(df, dict_scheme):
        # 数据类型处理
        if dict_scheme != {}:
            for i in dict_scheme.keys():
                df = df.withColumn(i, col(i).cast(dict_scheme[i]))
        return df
    
    def getInputVersion(df, table_name):
        # 如果 table在g_input_version中指定了version，则读取df后筛选version，否则使用传入的df
        version = g_input_version.get(table_name, '')
        if version != '':
            version_list =  version.replace(' ','').split(',')
            df = df.where(col('version').isin(version_list))
        return df
    
    def readInFile(table_name, dict_scheme={}):
        df = kwargs[table_name]
        df = dealToNull(df)
        df = dealScheme(df, dict_scheme)
        df = getInputVersion(df, table_name.replace('df_', ''))
        return df
    
    df_max_result = readInFile('df_max_result_backfill')
    df_max_result = df_max_result.where((col('date') >= model_month_left) & (col('date') <= model_month_right))
        
    df_prod_mapping = readInFile('df_prod_mapping')
           
    df_ims_sales_all = readInFile('df_ims_info')
    
    df_factor_all = readInFile('df_factor_raw')
    
    if factor_optimize != "True":
         return {"out_df": df_factor_all}
              
    # ============== 删除已有的s3中间文件 =============
    import boto3
    def deletePath(path_dir):
        file_name = path_dir.replace('//', '/').split('s3:/ph-platform/')[1]
        s3 = boto3.resource('s3', region_name='cn-northwest-1',
                            aws_access_key_id="AKIAWPBDTVEAEU44ZAGT",
                            aws_secret_access_key="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")
        bucket = s3.Bucket('ph-platform')
        bucket.objects.filter(Prefix=file_name).delete()
    deletePath(path_dir=f"{p_out + g_table_ims_v1_otherall}/version={run_id}/provider={project_name}/owner={owner}/")
    deletePath(path_dir=f"{p_out + g_table_ims_panel_max_out}/version={run_id}/provider={project_name}/owner={owner}/")
    deletePath(path_dir=f"{p_out + g_table_factor3}/version={run_id}/provider={project_name}/owner={owner}/")
    
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
    dict_cols_prod_map = {"通用名":["通用名", "标准通用名", "通用名_标准", "药品名称_标准", "s_molecule_name"], 
                          "min2":["min2", "min1_标准"],
                          "pfc":['pfc', 'packcode', 'pack_id', 'packid', 'packid'],
                          "标准商品名":["标准商品名", "商品名_标准", "s_molecule_name"],
                          "标准剂型":["标准剂型", "剂型_标准", 'form_std', 's_dosage'],
                          "标准规格":["标准规格", "规格_标准", 'specifications_std', '药品规格_标准', 's_pack'],
                          "标准包装数量":["标准包装数量", "包装数量2", "包装数量_标准", 'pack_number_std', 's_packnumber', "最小包装数量"],
                          "标准生产企业":["标准生产企业", "标准企业", "生产企业_标准", 'manufacturer_std', 's_corporation', "标准生产厂家"]
                         }
    
    df_prod_mapping = getTrueColRenamed(df_prod_mapping, dict_cols_prod_map, df_prod_mapping.columns)
    df_prod_mapping = dealScheme(df_prod_mapping, {"标准包装数量":"int", "pfc":"int"})
    
    df_prod_mapping = df_prod_mapping.withColumn("min2", func.regexp_replace("min2", "&amp;", "&")) \
                                    .withColumn("min2", func.regexp_replace("min2", "&lt;", "<")) \
                                    .withColumn("min2", func.regexp_replace("min2", "&gt;", ">"))
    df_prod_mapping = df_prod_mapping.select('通用名', '标准商品名', '标准剂型', '标准规格', '标准包装数量',
                                             
                                        '标准生产企业', 'min2', 'pfc') \
                                    .distinct() \
                                    .withColumnRenamed('通用名', 'molecule') \
                                    .withColumnRenamed('标准商品名', 'brand') \
                                    .withColumnRenamed('标准剂型', 'form') \
                                    .withColumnRenamed('标准规格', 'specifications') \
                                    .withColumnRenamed('标准包装数量', 'pack_number') \
                                    .withColumnRenamed('标准生产企业', 'manufacturer') \
                                    .withColumnRenamed('pfc', 'pack_id')
        
    
    # %%
    # =========== 数据执行 ============
    logger.debug("job3_factor_optimize")
    @udf(StringType())
    def cityChange(name):
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
    
    def lowerColumns(df):
        df = df.toDF(*[i.lower() for i in df.columns])
        return df
    
    def processMaxResult(df_max_result, df_factor, df_prod_mapping, market):
        # 3.1 max 数据
        df_max = df_max_result.where(col('doi') == market)
        df_max = df_max.join(df_factor, on='city', how='left')
        df_max = df_max.withColumn('predict_sales', func.when(col('panel') == 0, col('factor')*col('predict_sales')) \
                                                        .otherwise(col('predict_sales'))) \
                        .withColumn('citynew', col('city'))
    
        df_max = df_max.withColumn('citynew', cityChange(col('city')))
        df_max = df_max.withColumn('citynew', func.when(~col('citynew').isin(df_ims_info.select('city').distinct().toPandas()['city'].values.tolist()), 
                                                        func.lit('other')).otherwise(col('citynew')))
    
        df_max = df_max.join(df_prod_mapping.dropDuplicates(['min2']), df_max['prod_name']==df_prod_mapping['min2'], how='left')
        return df_max
    
    def processPanel(df_max, market):
        # 3.2 panel数据
        df_bll1 = df_max.where(col('panel') == 1)
        # 城市产品层面
        df_bll2 = df_bll1.groupBy('brand', 'citynew').agg(func.sum('predict_sales').alias('panel_Sales')) \
                    .withColumn('mkt', func.lit(market)) \
                    .withColumnRenamed('citynew', 'city')
        # 全国的市场
        df_bll3 = df_bll1.groupBy('citynew').agg(func.sum('predict_sales').alias('panel_mkt')) \
                    .withColumn('market', func.lit(market)) \
                    .withColumnRenamed('citynew', 'city')
        return [df_bll2, df_bll3]

    def getImsTop3(ims_df, df_prod_mapping, df_max2, df_max3, df_bll2, df_bll3):
        # 3.3 确定IMS三个最大的产品
        ims_df = ims_df.withColumn('brand_en', col('prd_desc')) \
                            .join(df_prod_mapping.select('pack_id', 'brand').dropDuplicates(['pack_id']), on='pack_id', how='left')

        ims_df = ims_df.groupBy('market', 'brand', 'city').agg(func.sum('ims_poi_vol').alias('prod')) \
                        .withColumn('brand', func.when(col('brand').isNull(), func.lit('none')).otherwise(col('brand')))

        # 全国的商品销量
        ims_part1 = ims_df.where(col('city') == 'CHPA')
        ims_part1_v1 = ims_part1.groupBy('market', 'brand').agg(func.sum('prod').alias('prod_chpa'))
        # 非other 省份的商品销量
        ims_part2 = ims_df.where(col('city') != 'CHPA')
        ims_part2_v1 = ims_part2.groupBy('market', 'brand').agg(func.sum('prod').alias('prod_nonchpa'))
        # other 省份的商品销量
        ims_v1_left = ims_part1_v1.join(ims_part2_v1, on=['market','brand'], how='left')
        ims_v1_left = ims_v1_left.fillna(0, 'prod_nonchpa') \
                                .withColumn('prod_other', col('prod_chpa')-col('prod_nonchpa')) \
                                .withColumn('city', func.lit('other'))
        ims_v1_left = ims_v1_left.withColumnRenamed('prod_chpa', 'chpa') \
                                .withColumnRenamed('prod_nonchpa', 'ims_city') \
                                .withColumnRenamed('prod_other', 'prod') \
                                .select('market','brand','city','prod')
        # other + 非other省份 每个城市的销量前三名产品
        ims_v1_otherall = ims_part2.union(ims_v1_left)
        ims_v1_otherall = ims_v1_otherall.withColumn('n',
                                func.row_number().over(Window.partitionBy('market', 'city').orderBy(col('prod').desc())))
        ims_v1_otherall = ims_v1_otherall.where(col('n') < 4) \
                                        .orderBy(col('market'), col('city'), col('prod').desc()) \
                                        .withColumnRenamed('prod', 'ims_prod').persist()

        ims_v1_mkt =  ims_part2.union(ims_v1_left) \
                                .groupBy('market','city').agg(func.sum('prod').alias('ims_mkt'))

        ims_panel_max = ims_v1_otherall.join(df_max2.withColumnRenamed('mkt', 'market'), 
                                            on=['market', 'brand', 'city'], how='left') \
                                        .join(df_bll2.withColumnRenamed('mkt', 'market'),
                                            on=['market', 'brand', 'city'], how='left')
        ims_panel_max = unpivot(ims_panel_max, ['market', 'brand', 'city', 'n'])
        ims_panel_max = ims_panel_max.withColumn('value', col('value').cast(DoubleType())) \
                                    .withColumn('new', func.concat(col('feature'), func.lit('_'), col('n'))) \
                                    .drop('brand', 'feature', 'n')
        ims_panel_max = ims_panel_max.groupBy('market', 'city').pivot('new').agg(func.sum('value')).fillna(0).persist()
        ims_panel_max = ims_panel_max.join(ims_v1_mkt, on=['market','city'], how='left') \
                                    .join(df_max3, on=['market','city'], how='left') \
                                    .join(df_bll3, on=['market','city'], how='left')
        ims_panel_max = ims_panel_max.fillna(0, ims_panel_max.columns[2:])
        return [ims_v1_otherall, ims_panel_max]
        
    # 1. max 文件处理
    df_max_result = df_max_result.where((col('date') >= model_month_left) & (col('date') <= model_month_right))    
    # 2. 对每个市场优化factor
    for market in all_models:
        logger.debug("当前market为:" + str(market))
        # 数据执行
        df_ims_info = df_ims_sales_all.where(col('model') == market)
        df_factor = df_factor_all.where(col('doi') == market)
        ims_df = df_ims_info.withColumn('market', func.lit(market))
    
        # 3.1 max 数据
        df_max = processMaxResult(df_max_result, df_factor, df_prod_mapping, market)
    
        # 城市产品层面
        df_max2 = df_max.groupBy('brand', 'citynew').agg(func.sum('predict_sales').alias('max_prod')) \
                    .withColumn('mkt', func.lit(market)) \
                    .withColumnRenamed('citynew', 'city')
        # 全国的市场
        df_max3 = df_max.groupBy('citynew').agg(func.sum('predict_sales').alias('max_mkt')) \
                    .withColumn('market', func.lit(market)) \
                    .withColumnRenamed('citynew', 'city')
    
        # 3.2 panel数据
        l_panel = processPanel(df_max, market)
        # 城市产品层面
        df_bll2 = l_panel[0]
        # 全国的市场
        df_bll3 = l_panel[1]
    
        # 3.3 确定IMS三个最大的产品
        l_ims_result = getImsTop3(ims_df, df_prod_mapping, df_max2, df_max3, df_bll2, df_bll3)
        # top3产品信息
        ims_v1_otherall = l_ims_result[0]
        # ims_panel_max
        ims_panel_max = l_ims_result[1]
        
        l_need_cols = ['ims_prod_2', 'max_prod_2', 'panel_Sales_2', 'ims_prod_3', 'max_prod_3', 'panel_Sales_3']
        for i in l_need_cols:
            if i not in ims_panel_max.columns:
                ims_panel_max = ims_panel_max.withColumn(i, func.lit(0))
                
        # ==== 输出 ====
        ims_v1_otherall = ims_v1_otherall.withColumn('doi', func.lit(market))
        ims_v1_otherall = lowerColumns(ims_v1_otherall)
        AddTableToGlue(df=ims_v1_otherall, database_name_of_output=g_database_temp, table_name_of_output=g_table_ims_v1_otherall, 
                           path_of_output_file=p_out, mode=out_mode) \
                    .add_info_of_partitionby({"version":run_id,"provider":project_name,"owner":owner})
        
        # 3.4 优化 
        logger.debug("cvxpy优化")
        schema = deepcopy(ims_panel_max.schema)
        schema.add("factor", DoubleType())
        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
        def cvxpy_func(pdf):
            import numpy as np
            import cvxpy as cp
    
            f = cp.Variable()
            prob = cp.Problem(cp.Minimize(cp.maximum(cp.abs((f*(pdf['max_prod_1'][0]-
                                              pdf['panel_Sales_1'][0])+
                                           pdf['panel_Sales_1'][0])/pdf['ims_prod_1'][0]-1),
                                    cp.abs((f*(pdf['max_prod_2'][0]-
                                              pdf['panel_Sales_2'][0])+
                                           pdf['panel_Sales_2'][0])/pdf['ims_prod_2'][0]-1),
                                    cp.abs((f*(pdf['max_prod_3'][0]-
                                              pdf['panel_Sales_3'][0])+
                                           pdf['panel_Sales_3'][0])/pdf['ims_prod_3'][0]-1),
                                    cp.abs((f*(pdf['max_mkt'][0]-
                                              pdf['panel_mkt'][0])+
                                           pdf['panel_mkt'][0])/pdf['ims_mkt'][0]-1))),
                       [cp.abs((f*(pdf['max_mkt'][0]-pdf['panel_mkt'][0])+
                                   pdf['panel_mkt'][0])/pdf['ims_mkt'][0]-1) <= 0.05])
            prob.solve(solver = cp.ECOS)
    
            return pdf.assign(factor = f.value)
    
        ims_panel_max_out = ims_panel_max.groupby('market','city').apply(cvxpy_func)
        ims_panel_max_out = ims_panel_max_out.withColumn('factor', 
                                                func.when((col('factor').isNull()) | (col('factor') < 0 ), func.lit(0)) \
                                                    .otherwise(col('factor')))
    
        ims_panel_max_out = ims_panel_max_out.withColumn('gap1', 
                (col('factor')*(col('max_prod_1') - col('panel_Sales_1')) + col('panel_Sales_1')) / col('ims_prod_1') -1)
        ims_panel_max_out = ims_panel_max_out.withColumn('gap2', 
                (col('factor')*(col('max_prod_2') - col('panel_Sales_2')) + col('panel_Sales_2')) / col('ims_prod_2') -1)
        ims_panel_max_out = ims_panel_max_out.withColumn('gap3', 
                (col('factor')*(col('max_prod_3') - col('panel_Sales_3')) + col('panel_Sales_3')) / col('ims_prod_3') -1)
        ims_panel_max_out = ims_panel_max_out.withColumn('gap_mkt', 
                (col('factor')*(col('max_mkt') - col('panel_mkt')) + col('panel_mkt')) / col('ims_mkt') -1)
    
        # ==== 输出 ====
        ims_panel_max_out = ims_panel_max_out.withColumn('doi', func.lit(market))
        ims_panel_max_out = lowerColumns(ims_panel_max_out)
        AddTableToGlue(df=ims_v1_otherall, database_name_of_output=g_database_temp, table_name_of_output=g_table_ims_panel_max_out, 
                           path_of_output_file=p_out, mode=out_mode) \
                    .add_info_of_partitionby({"version":run_id,"provider":project_name,"owner":owner})
        
        
        df_factor1 = df_factor
        df_factor1 = df_factor1.withColumnRenamed('factor', 'factor1')
        df_factor2 = ims_panel_max_out.select('city','factor').withColumnRenamed('factor', 'factor2')
        df_factor3 = df_factor1.join(df_factor2, on='city', how='left')
    
        l_factor2_city = df_factor2.select('city').distinct().toPandas()['city'].values.tolist()
        
        def processCityFactor(l_factor2_city, df_factor2, df_factor3):
            dict_city_map = {"福厦泉市":["福州市","厦门市","泉州市"], "珠三角市":["珠海市","东莞市","中山市","佛山市"], "浙江市":["绍兴市","嘉兴市","台州市","金华市"], "苏锡市":["苏州市","无锡市"]}
            for i_city, l_map_city in dict_city_map.items():
                if i_city in l_factor2_city:
                    value = df_factor2.where(col('city')==i_city).select('factor2').toPandas()['factor2'][0]
                    df_factor3 = df_factor3.withColumn('factor2', func.when(col('city').isin(l_map_city), func.lit(value)) \
                                                            .otherwise(col('factor2')))
            return df_factor3
            
        df_factor3 = processCityFactor(l_factor2_city, df_factor2, df_factor3)
    
        value_other = df_factor2.where(col('city')=='other').select('factor2').toPandas()['factor2'][0]    
        df_factor3 = df_factor3.withColumn('factor2', func.when(col('factor2').isNull(), func.lit(value_other)) \
                                                    .otherwise(col('factor2')))
    
        df_factor3 = df_factor3.withColumn('factor', col('factor1') * col('factor2'))
        df_factor3 = df_factor3.withColumn('factor', func.when(col('factor') > 4, func.lit(4)).otherwise(col('factor')))
        
        # ==== 输出 ====
        df_factor3 = df_factor3.withColumn('doi', func.lit(market))
        df_factor3 = lowerColumns(df_factor3)
        AddTableToGlue(df=df_factor3, database_name_of_output=g_database_temp, table_name_of_output=g_table_factor3, 
                           path_of_output_file=p_out, mode=out_mode) \
                    .add_info_of_partitionby({"version":run_id,"provider":project_name,"owner":owner})


    # %%
    # =========== 数据输出 =============   
    df_result = spark.sql("SELECT * FROM %s.%s WHERE version='%s' AND provider='%s' AND  owner='%s'" 
                                 %(g_database_temp, g_table_factor3, run_id, project_name, owner))
    df_result = lowerColumns(df_result)
    df_result = df_result.drop('version', 'provider', 'owner')
    return {"out_df":df_result} 

