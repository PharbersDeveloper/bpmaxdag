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
    universe_choice = kwargs['universe_choice']
    all_models = kwargs['all_models']
    weight_upper = kwargs['weight_upper']
    job_choice = kwargs['job_choice']
    g_input_version = kwargs['g_input_version']
    ### input args ###
    
    ### output args ###
    p_out = kwargs['p_out']
    out_mode = kwargs['out_mode']
    run_id = kwargs['run_id'].replace(":","_")
    owner = kwargs['owner']
    project_name = kwargs['project_name']
    g_database_temp = kwargs['g_database_temp']
    ### output args ###

    
    
    
    
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, col, udf
    
    from scipy.stats import ranksums, mannwhitneyu
    import pandas as pd
    import numpy as np    
    from phcli.ph_tools.addTable.addTableToGlue import AddTableToGlue
    
    # %%
    # =========== 参数处理 =========== 
    # 是否运行此job    
    if job_choice != "weight_default":
        raise ValueError('不运行weight_default')
    all_models = all_models.replace(", ",",").split(",")
    weight_upper = float(weight_upper)
       
    # 市场的universe文件
    def getVersionDict(str_choice):
        dict_choice = {}
        if str_choice != "Empty":
            for each in str_choice.replace(", ",",").split(","):
                market_name = each.split(":")[0]
                version_name = each.split(":")[1]
                dict_choice[market_name]=version_name
        return dict_choice
    dict_universe_choice = getVersionDict(universe_choice)
    
    g_table_result = 'PHA_weight_default'
    
    # ============== 删除已有的s3中间文件 =============
    import boto3
    def deletePath(path_dir):
        file_name = path_dir.replace('//', '/').split('s3:/ph-platform/')[1]
        s3 = boto3.resource('s3', region_name='cn-northwest-1',
                            aws_access_key_id="AKIAWPBDTVEAEU44ZAGT",
                            aws_secret_access_key="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")
        bucket = s3.Bucket('ph-platform')
        bucket.objects.filter(Prefix=file_name).delete()
    deletePath(path_dir=f"{p_out + g_table_result}/version={run_id}/provider={project_name}/owner={owner}/")
    
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

    def lowCol(df):
        df = df.toDF(*[c.lower() for c in df.columns])
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
        df = lowCol(df)
        df = dealScheme(df, dict_scheme)
        df = getInputVersion(df, table_name.replace('df_', ''))
        return df
            
    def getUniverse(market, dict_universe_choice):
        if market in dict_universe_choice.keys():
            df_universe = readInFile('df_universe_other').where(col('version')==dict_universe_choice[market])
        else:
            df_universe = readInFile('df_universe_base')
        return df_universe

    # %% 
    # =========== 数据清洗 =============
    #logger.debug('数据清洗-start')
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
            print(true_colname)
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
    
    def cleanUniverse(df_universe):
        dict_cols_universe = {"City_Tier_2010":["City_Tier", "CITYGROUP", "City_Tier_2010"], "Panel_ID":["Panel_ID", "PHA"]}
        df_universe = getTrueColRenamed(df_universe, dict_cols_universe, df_universe.columns)
        df_universe = dealScheme(df_universe, dict_scheme = {"Est_DrugIncome_RMB":"double"})
        df_universe = df_universe.select("Panel_ID", "City", "Province", "City_Tier_2010", "HOSP_NAME", "PANEL", "BEDSIZE", "Seg", "Est_DrugIncome_RMB").distinct() \
                                    .withColumn('Seg', col('Seg').cast('int').cast('string'))
        return df_universe
    
    # %%
    # ========  数据分析  ========
    
    for index, market in enumerate(all_models):
        df_universe = getUniverse(market, dict_universe_choice)
        df_universe = cleanUniverse(df_universe)
        df_universe = df_universe.fillna(0, 'Est_DrugIncome_RMB') \
                            .withColumn('Est_DrugIncome_RMB', func.when(func.isnan('Est_DrugIncome_RMB'), 0).otherwise(col('Est_DrugIncome_RMB')))
        
        # 数据处理
        df_universe_panel = df_universe.where(col('PANEL') == 1).select('Panel_ID', 'Est_DrugIncome_RMB', 'Seg')
        df_universe_non_panel = df_universe.where(col('PANEL') == 0).select('Est_DrugIncome_RMB', 'Seg', 'City', 'Province')
        
        seg_multi_cities = df_universe.select('Seg', 'City', 'Province').distinct() \
                                .groupby('Seg').count()
        seg_multi_cities = seg_multi_cities.where(col('count') > 1).select('Seg').toPandas()['Seg'].tolist()
    
        df_universe_m = df_universe_panel.where(col('Seg').isin(seg_multi_cities)) \
                                    .withColumnRenamed('Est_DrugIncome_RMB', 'Est_DrugIncome_RMB_x') \
                                    .join(df_universe_non_panel, on='Seg', how='inner')
        
        # 秩和检验获得p值
        schema = StructType([
            StructField("Panel_ID", StringType(), True),
            StructField("City", StringType(), True),
            StructField("Province", StringType(), True),
            StructField("pvalue", DoubleType(), True)
            ])
    
        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
        def wilcoxtest(pdf):
            # 秩和检验
            Panel_ID = pdf['Panel_ID'][0]
            City = pdf['City'][0]
            Province = pdf['Province'][0]
            a = pdf['Est_DrugIncome_RMB_x'].drop_duplicates().values.astype(float)
            b = pdf['Est_DrugIncome_RMB'].values.astype(float)
            pvalue = round(mannwhitneyu(a, b, alternative="two-sided")[1],6) # 等同于R中的wilcox.test()
            return pd.DataFrame([[Panel_ID] + [City] + [Province] + [pvalue]], columns=["Panel_ID", "City", "Province", "pvalue"])
    
        df_universe_m_wilcox = df_universe_m.groupby('Panel_ID', 'City', 'Province') \
                                    .apply(wilcoxtest)
        
        df_universe_m_maxmin = df_universe_m_wilcox.groupby('Panel_ID') \
                                            .agg(func.min('pvalue').alias('min'), func.max('pvalue').alias('max'))
        
        # 计算weight
        df_universe_m_weight = df_universe_m_wilcox.join(df_universe_m_maxmin, on='Panel_ID', how='left') \
                                            .withColumn('Weight', 
                        (col('pvalue') - col('min'))/(col('max') - col('min'))*(weight_upper-1/weight_upper) + 1/weight_upper)
    
        df_universe_m_weight = df_universe_m_weight.fillna(1, 'Weight')
        
        df_weight_out = df_universe_m_weight.withColumn('DOI', func.lit(market)) \
                                    .withColumnRenamed('Panel_ID', 'PHA') \
                                    .select('Province', 'City', 'DOI', 'Weight', 'PHA')
        
        # 结果输出
        def lowerColumns(df):
            df = df.toDF(*[i.lower() for i in df.columns])
            return df
        df_weight_out = lowerColumns(df_weight_out)
        
        AddTableToGlue(df=df_weight_out, database_name_of_output=g_database_temp, table_name_of_output=g_table_result, 
                           path_of_output_file=p_out, mode=out_mode) \
                    .add_info_of_partitionby({"version":run_id,"provider":project_name,"owner":owner})
            

    # %%
    # =========== 数据输出 =============
    # 读回
    df_out = spark.sql("SELECT * FROM %s.%s WHERE version='%s' AND provider='%s' AND  owner='%s'" 
                                 %(g_database_temp, g_table_result, run_id, project_name, owner))
    df_out = df_out.drop('version', 'provider', 'owner')
    
    return {"out_df":df_out}