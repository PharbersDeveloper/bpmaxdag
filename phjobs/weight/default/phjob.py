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
    universe_choice = kwargs['universe_choice']
    all_models = kwargs['all_models']
    weight_upper = kwargs['weight_upper']
    job_choice = kwargs['job_choice']
    test = kwargs['test']
    ### input args ###
    
    ### output args ###
    c = kwargs['c']
    d = kwargs['d']
    ### output args ###

    
    
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, col, udf
    
    from scipy.stats import ranksums, mannwhitneyu
    import pandas as pd
    import numpy as np    
    # %%
    # project_name = "Gilead"
    # universe_choice = "乙肝:universe_传染,乙肝_2:universe_传染,乙肝_3:universe_传染,安必素:universe_传染"
    # all_models = "乙肝,乙肝_2,乙肝_3,安必素"
    # weight_upper = "1.25"
    # job_choice = "weight_default"
    # test = "True"
    # %%
    # 是否运行此job
    if test != "False" and test != "True":
        logger.info('wrong input: test, False or True') 
        raise ValueError('wrong input: test, False or True')
    
    if job_choice != "weight_default":
         raise ValueError('不运行weight_default')
    
    # 输入
    universe_choice_dict={}
    if universe_choice != "Empty":
        for each in universe_choice.replace(" ","").split(","):
            market_name = each.split(":")[0]
            universe_name = each.split(":")[1]
            universe_choice_dict[market_name]=universe_name
    
    all_models = all_models.replace(", ",",").split(",")
    weight_upper = float(weight_upper)
    
    # 输出
    project_path = project_path = max_path + "/" + project_name
    if test == "True":
        weight_default_path = max_path + "/" + project_name + '/test/PHA_weight_default'
    else:
        weight_default_path = max_path + "/" + project_name + '/PHA_weight_default'
    

    # %%
    # ====  数据分析  ====
    
    for index, market in enumerate(all_models):
        if market in universe_choice_dict.keys():
            universe_path = project_path + '/' + universe_choice_dict[market]
        else:
            universe_path = project_path + '/universe_base'
    
        universe = spark.read.parquet(universe_path)
        universe = universe.fillna(0, 'Est_DrugIncome_RMB')
        
        # 数据处理
        universe_panel = universe.where(col('PANEL') == 1).select('Panel_ID', 'Est_DrugIncome_RMB', 'Seg')
        universe_non_panel = universe.where(col('PANEL') == 0).select('Est_DrugIncome_RMB', 'Seg', 'City', 'Province')
        
        seg_multi_cities = universe.select('Seg', 'City', 'Province').distinct() \
                                .groupby('Seg').count()
        seg_multi_cities = seg_multi_cities.where(col('count') > 1).select('Seg').toPandas()['Seg'].tolist()
    
        universe_m = universe_panel.where(col('Seg').isin(seg_multi_cities)) \
                                    .withColumnRenamed('Est_DrugIncome_RMB', 'Est_DrugIncome_RMB_x') \
                                    .join(universe_non_panel, on='Seg', how='inner')
        
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
    
        universe_m_wilcox = universe_m.groupby('Panel_ID', 'City', 'Province') \
                                    .apply(wilcoxtest)
        
        universe_m_maxmin = universe_m_wilcox.groupby('Panel_ID') \
                                            .agg(func.min('pvalue').alias('min'), func.max('pvalue').alias('max'))
        
        # 计算weight
        universe_m_weight = universe_m_wilcox.join(universe_m_maxmin, on='Panel_ID', how='left') \
                                            .withColumn('Weight', 
                        (col('pvalue') - col('min'))/(col('max') - col('min'))*(weight_upper-1/weight_upper) + 1/weight_upper)
    
        universe_m_weight = universe_m_weight.fillna(1, 'Weight')
        
        weight_out = universe_m_weight.withColumn('DOI', func.lit(market)) \
                                    .withColumnRenamed('Panel_ID', 'PHA') \
                                    .select('Province', 'City', 'DOI', 'Weight', 'PHA')
        
        # 结果输出
        if index ==0:
            weight_out = weight_out.repartition(1)
            weight_out.write.format("parquet") \
                .mode("overwrite").save(weight_default_path)
        else:
            weight_out = weight_out.repartition(1)
            weight_out.write.format("parquet") \
                .mode("append").save(weight_default_path)
            

