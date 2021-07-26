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
    time_left = kwargs['time_left']
    time_right = kwargs['time_right']
    molecule = kwargs['molecule']
    molecule_sep = kwargs['molecule_sep']
    atc = kwargs['atc']
    project = kwargs['project']
    doi = kwargs['doi']
    out_suffix = kwargs['out_suffix']
    data_type = kwargs['data_type']
    market_define = kwargs['market_define']
    out_path = kwargs['out_path']
    run_id = kwargs['run_id']
    owner = kwargs['owner']
    g_input_version = kwargs['g_input_version']
    g_database_input = kwargs['g_database_input']
    g_database_result = kwargs['g_database_result']
    ### input args ###
    
    ### output args ###
    c = kwargs['c']
    d = kwargs['d']
    ### output args ###

    
    
    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col
    import json
    import boto3
    import time
    
    # %%
    # time_left = "202001"
    # time_right = "202010"
    # project = "AZ"
    # doi = "Respules_Market, HTN_Market"
    # out_suffix = "test"
    # market_define = "AZ"
    
    # time_left = "201801"
    # time_right = "201912"
    # project = "Servier"
    # molecule = "二甲双胍, 格列喹酮"
    # out_suffix = "test"    # %%
    # a. 输入
    if data_type != "max" and data_type != "raw":
        phlogger.error('wrong input: data_type, max or raw') 
        raise ValueError('wrong input: data_type, max or raw')
    
    if data_type == 'max':
        g_extract_table = 'max_result'
    elif data_type == 'raw':
        g_extract_table = 'rawdata_standard'
    
    if out_suffix == "Empty":
        raise ValueError('out_suffix: missing')
    
    # 提数条件
    time_left = int(time_left)
    time_right = int(time_right)
    
    if molecule == "Empty":
        molecule = [] 
    else:
        if molecule_sep == "Empty": 
            molecule = molecule.replace(" ","").split(",")
        else:
            molecule = molecule.replace(" ","").split(molecule_sep)
    
    if atc == "Empty":
        atc = []
    else:
        atc = atc.replace(" ","").split(",")
    
    if len(set(len(i) for i in atc)) >1 :
        raise ValueError('atc length diff')
    
    if project == "Empty":
        project = []
    else:
        project = project.replace(" ","").split(",")
    
    if doi == "Empty":
        doi = []
    else:
        doi = doi.replace(" ","").split(",")
        if market_define == "Empty":
            raise ValueError('没有定义 market_define')
        if project == "Empty":
            raise ValueError('没有指定 project')
            
    # b. 输出
    outdir = run_id + "_" + out_suffix
    out_extract_data_path = out_path + "/" + outdir + "/" + run_id + "_" + out_suffix + '.csv'
    report_a_path = out_path + "/" + outdir + "/report_a.csv"
    # %% 
    # 输入数据读取
    dict_input_version = json.loads(g_input_version)
    logger.debug(dict_input_version)
    
    df_project_rank =  spark.sql("SELECT * FROM %s.project_rank WHERE provider='%s' AND version='%s'" 
                             %(g_database_input, 'common', dict_input_version['project_rank'])) \
                            .drop('owner', 'provider', 'version')
    
    df_market_define =  spark.sql("SELECT * FROM %s.market_define WHERE provider='%s' AND version='%s'" 
                             %(g_database_input, 'common', dict_input_version['market_define'])) \
                            .drop('owner', 'provider', 'version')
    
    df_project_for_extract =  spark.sql("SELECT * FROM %s.project_for_extract WHERE provider='%s' AND version='%s'" 
                             %(g_database_input, 'common', dict_input_version['project_for_extract'])) \
                            .drop('owner', 'provider', 'version')
    
    df_max_result_all = spark.sql("SELECT * FROM %s.%s WHERE filetype='all'" 
                             %(g_database_result, g_extract_table)) \
                            .drop('owner', 'version', 'filetype') \
                            .withColumnRenamed("provider", "project")
    
    df_max_result_brief = spark.sql("SELECT * FROM %s.%s WHERE filetype='brief'" 
                             %(g_database_result, g_extract_table)) \
                            .drop('owner', 'version', 'filetype') \
    
    if data_type == 'max':
        df_max_result_brief = df_max_result_brief.select("DATE", "标准通用名", "ATC", "DOI", "PACK_ID", "provider") \
                                                    .withColumnRenamed("provider", "project")

    # %%
    # ================ 数据执行 ==================
    '''
    raw 和 max 区别：
    1. raw：Sales，Units 不处理，就用原始的数据
    2. report A 去重依据：
        max：月份数-项目排名
        raw: 月份数-医院数-项目排名；增加了source来源统计
    3. 提数结果
        raw：增加了医院ID，原始医院名，PHA，PHA医院名
    '''
    
    # 一. 文件准备
    # 1. 项目排名文件
    project_rank = df_project_rank.withColumnRenamed("项目", "project") \
                        .withColumn("排名", col("排名").cast(IntegerType())) \
                        .withColumnRenamed("排名", "project_score")
    
    # 2. Market_definition_pack_level 市场名 packid水平
    if project and doi and market_define:
        df_market_define = df_market_define.withColumn('PACK_ID', col('PACK_ID').cast(IntegerType())) \
                                    .select('PACK_ID', 'Market', 'market_define')
        
        df_market_packid = df_market_define.where(col('market_define') == market_define) \
                                        .where(col('Market').isin(doi)) \
                                        .select('PACK_ID', 'Market').distinct()

    # %%
    # 二. 根据 max_standard_brief_all 确定最终提数来源
    
    # 1. 根据df_project_for_extract， 合并项目brief，生成max_standard_brief_all
    
    # 如果指定了project，那么只读取该项目的brief文件即可        
    if project:
        project_all = project
    else:
        project_all = df_project_for_extract.toPandas()["project"].tolist()
    
        
    df_max_standard_brief_all = df_max_result_brief.where(col('project').isin(project_all)) \
                                            .fillna(0, 'PACK_ID')

    # %%
    # 2. 根据提数需求获取 max_filter_path_month
    
    # 筛选,获取符合条件的项目和月份
    df_max_filter_list = df_max_standard_brief_all.where((col('Date') >= time_left) & (col('Date') <= time_right))
    if project:
        df_max_filter_list = df_max_filter_list.where(col('project').isin(project))
    if doi:
        df_max_filter_list = df_max_filter_list.join(df_market_packid, on='PACK_ID', how='inner')
    if atc:
        if max([len(i) for i in atc]) == 3:
            df_max_filter_list = df_max_filter_list.withColumn("ATC", func.substring(col('ATC'), 0, 3)).distinct()
        elif max([len(i) for i in atc]) == 4:
            df_max_filter_list = df_max_filter_list.withColumn("ATC", func.substring(col('ATC'), 0, 4)).distinct()
        df_max_filter_list = df_max_filter_list.where(col('ATC').isin(atc))
    if molecule:
        df_max_filter_list = df_max_filter_list.where(col('标准通用名').isin(molecule))

    # %%
    # 3. 注释项目排名
    df_max_filter_list = df_max_filter_list.join(project_rank, on="project", how="left").persist()
    
    # 4. 根据月份数以及项目排名进行去重，确定最终提数来源，生成报告 report_a 
    report = df_max_filter_list.select("project","project_score","标准通用名", "ATC", "Date") \
                            .distinct() \
                            .groupby(["标准通用名", "ATC", "project","project_score"]).count() \
                            .withColumnRenamed("count", "months_num") \
                            .persist()
    
    # 分子最大月份数, 月份最全-得分
    df_months_max = report.groupby("标准通用名", "ATC").agg(func.max("months_num").alias("max_month"))
    
    report = report.join(df_months_max, on=["标准通用名", "ATC"], how="left")
    report = report.withColumn("drop_for_months", func.when(report.months_num == report.max_month, func.lit(0)).otherwise(func.lit(1)))
    
    # 对于raw_data 医院数量作为第二去重条件
    if data_type == 'raw':
        # 数据来源
        Source_window = Window.partitionBy("project", "标准通用名").orderBy(func.col('Source'))
        rank_window = Window.partitionBy("project", "标准通用名").orderBy(func.col('Source').desc())
    
        Source = df_max_filter_list.select("project", "标准通用名", 'Source').distinct() \
                                .select("project", "标准通用名",
                                         func.collect_list(func.col('Source')).over(Source_window).alias('Source'),
                                         func.rank().over(rank_window).alias('rank')).persist()
        Source = Source.where(Source.rank == 1).drop('rank')
        Source = Source.withColumn('Source', func.concat_ws(',', func.col('Source')))
    
        report = report.join(Source, on=["project", "标准通用名"], how='left').persist()                                
    
        # 医院数统计                    
        PHA_num = df_max_filter_list.select("project","project_score","标准通用名", "ATC", "PHA") \
                            .distinct() \
                            .groupby(["标准通用名", "ATC", "project","project_score"]).count() \
                            .withColumnRenamed("count", "PHA_num") \
                            .persist()
        report = report.join(PHA_num, on=["标准通用名", "ATC", "project", "project_score"], how="left").persist()
    
        # 月份相同的 计算医院数量最大的
        PHA_num_max = report.where(report.drop_for_months == 0) \
                    .groupby("标准通用名").agg(func.max("PHA_num").alias("max_PHA_num"))
        report = report.join(PHA_num_max, on="标准通用名", how="left")
    
        report = report.withColumn("drop_for_PHA", func.when(report.PHA_num == report.max_PHA_num, func.lit(0)).otherwise(func.lit(1)))
        report = report.withColumn("drop_for_PHA", func.when(report.drop_for_months == 0, report.drop_for_PHA).otherwise(None))
    
        # 项目得分
        score_max = report.where(report.drop_for_PHA == 0) \
                    .groupby("标准通用名").agg(func.min("project_score").alias("max_score"))
        report = report.join(score_max, on="标准通用名", how="left")
        report = report.withColumn("drop_for_score", func.when(report.project_score == report.max_score, func.lit(0)).otherwise(func.lit(1)))
        report = report.withColumn("drop_for_score", func.when(report.drop_for_PHA == 0, report.drop_for_score).otherwise(None))
    elif data_type == 'max':
        # 项目得分
        score_max = report.where(report.drop_for_months == 0) \
                    .groupby("标准通用名").agg(func.min("project_score").alias("max_score"))
        report = report.join(score_max, on="标准通用名", how="left")
        report = report.withColumn("drop_for_score", func.when(report.project_score == report.max_score, func.lit(0)).otherwise(func.lit(1)))
        report = report.withColumn("drop_for_score", func.when(report.drop_for_months == 0, report.drop_for_score).otherwise(None))
    
    # 时间范围range，最小月-最大月
    df_time_range = df_max_filter_list.select("project","标准通用名", "ATC", "Date") \
                    .distinct() \
                    .groupby(["project","标准通用名", "ATC"]).agg(func.min("Date").alias("min_time"), func.max("Date").alias("max_time"))
    df_time_range = df_time_range.withColumn("time_range", func.concat(col('min_time'), func.lit("_"), col('max_time')))
    
    # report_a生成
    # report_a.withColumn("time_range", func.lit(str(time_left) + '_' + str(time_right)))
    report_a = report.drop("max_score", "max_month") \
                .join(df_time_range.drop("min_time", "max_time"), on=["project","标准通用名", "ATC"], how="left")
    # 列名顺序调整
    if data_type == 'raw':
        report_a = report_a.select("project", "ATC", "标准通用名", 'Source', "time_range", "months_num", "drop_for_months", 
                                    "PHA_num", "drop_for_PHA", "project_score", "drop_for_score")
        report_a = report_a.orderBy(["标准通用名", "months_num", "PHA_num", "project_score"], ascending=[0, 0, 0, 1])
    else:
        report_a = report_a.select("project", "ATC", "标准通用名", "time_range", "months_num", "drop_for_months", "project_score", "drop_for_score")
        report_a = report_a.orderBy(["标准通用名", "months_num", "project_score"], ascending=[0, 0, 1])               
    
    report_a = report_a.withColumn("flag", func.when(report_a.drop_for_score == 0, func.lit(1)).otherwise(func.lit(None)))
    # 输出report_a            
    report_a = report_a.repartition(1)
    report_a.write.format("csv").option("header", "true") \
        .mode("overwrite").save(report_a_path)
    
    # 重新读入，否则当数据量大的时候后面的join report_a 报错
    report_a = spark.read.csv(report_a_path, header=True)
    
    # 根据 report_a 去重
    if atc:
        cols_list = ["ATC", "标准通用名", "project"]
    else:
        cols_list = ["标准通用名", "project"]
    df_max_filter_list = df_max_filter_list.join(report_a.where(report_a.flag == 1).select(cols_list).distinct(), 
                                on=[*cols_list], 
                                how="inner").persist()

    # %%
    # 三. 原始数据提取
    df_max_filter_raw = df_max_result_all.join(df_max_filter_list.select('project', 'DATE').distinct(), on=['project', 'DATE'], how='inner')
        
    # 过滤数据
    if doi:
        df_max_filter_raw = df_max_filter_raw.join(df_market_packid, on='PACK_ID', how='inner') \
                            .drop('DOI').withColumnRenamed('Market', 'DOI')
    if atc:
        if max([len(i) for i in atc]) == 3:
            df_max_filter_raw = df_max_filter_raw.withColumn("ATC", func.substring(col('ATC'), 0, 3)).distinct()
        elif max([len(i) for i in atc]) == 4:
            df_max_filter_raw = df_max_filter_raw.withColumn("ATC", func.substring(col('ATC'), 0, 4)).distinct()
        df_max_filter_raw = df_max_filter_raw.where(col('ATC').isin(atc))
    if molecule:
        df_max_filter_raw = df_max_filter_raw.where(col('标准通用名').isin(molecule))
        
        
    # 根据 report_a 去重
    if atc:
        df_max_filter_raw = df_max_filter_raw.join(report_a.where(col('flag') == 1).select("ATC", "标准通用名", "project").distinct(), 
                                    on=["ATC", "标准通用名", "project"], 
                                    how="inner").persist()
    else:
        df_max_filter_raw = df_max_filter_raw.join(report_a.where(col('flag') == 1).select("标准通用名", "project").distinct(), 
                                    on=["标准通用名", "project"], 
                                    how="inner").persist()
        

    # %%
    # 4. 注释项目排名
    df_max_filter_out = df_max_filter_raw.join(project_rank, on="project", how="left").persist()
    
    # 5. 原始提数结果
    if data_type == 'max':
        out_cols = ["project", "project_score", "Date", "ATC", "标准通用名", "标准商品名", "标准剂型", "标准规格", 
                "标准包装数量", "标准生产企业", "标准省份名称", "标准城市名称", "DOI", "PACK_ID", "Predict_Sales", "Predict_Unit", "PANEL"]
    elif data_type == 'raw':
        out_cols = ["project", "project_score", "ID", "Raw_Hosp_Name", "PHA", "PHA医院名称" ,"Date", "ATC", "标准通用名", 
                    "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业", "标准省份名称", "标准城市名称", 
                    "DOI", "PACK_ID", "Sales", "Units", "Units_Box"]
    df_max_filter_out = df_max_filter_out.select(out_cols).distinct()
    
    if data_type == 'max':
        # Sales，Units 处理
        '''
        包装数量为空的是others， Sales 或者 Units 可以为0
        包装数量不为空的，Sales和Units只要有一列为0，那么都调整为0；Units先四舍五入为整数，然后变化的系数乘以Sales获得新的Sales
        Sales 保留两位小数
        负值调整为0
        去掉 Sales，Units 同时为0的行
        '''
        df_max_filter_out = df_max_filter_out.withColumn("Predict_Sales", col("Predict_Sales").cast(DoubleType())) \
                                .withColumn("Predict_Unit", col("Predict_Unit").cast(DoubleType()))
    
        df_max_filter_out = df_max_filter_out.withColumn("Units", func.when((~col("标准包装数量").isNull()) & (col('Predict_Unit') <= 0), func.lit(0)) \
                                                                        .otherwise(func.round(col('Predict_Unit'), 0)))
    
        df_max_filter_out = df_max_filter_out.withColumn("p", col('Units')/col('Predict_Unit'))
        df_max_filter_out = df_max_filter_out.withColumn("p", func.when((~col("标准包装数量").isNull()) & (col("p").isNull()), func.lit(0)) \
                                                            .otherwise(col("p")))
        df_max_filter_out = df_max_filter_out.withColumn("p", func.when((col("标准包装数量").isNull()) & (col("p").isNull()), func.lit(1)) \
                                                            .otherwise(col("p")))
    
        df_max_filter_out = df_max_filter_out.withColumn("Sales", col("Predict_Sales") * col("p"))
    
        df_max_filter_out = df_max_filter_out.withColumn("Sales", func.round(col('Sales'), 2)) \
                                    .withColumn("Units", col("Units").cast(IntegerType())) \
                                    .drop("Predict_Unit", "Predict_Sales", "p")
    
        # 负值调整为0
        df_max_filter_out = df_max_filter_out.withColumn("Sales", func.when(col('Sales') < 0 , func.lit(0)).otherwise(col('Sales')))
        df_max_filter_out = df_max_filter_out.withColumn("Units", func.when(col('Sales') == 0, func.lit(0)).otherwise(col('Units')))
    
        # 去掉 Sales，Units 同时为0的行
        df_max_filter_out_1 = df_max_filter_out.where(col("标准包装数量").isNull())
        df_max_filter_out_2 = df_max_filter_out.where((~col("标准包装数量").isNull()) & (col('Sales') != 0) & (col('Units') != 0))
    
        df_max_filter_out =  df_max_filter_out_1.union(df_max_filter_out_2)   
    
    
    # 输出提数结果
    if data_type == 'max':
        if doi:
            out_cols = ["project", "project_score", "Date", "ATC", "标准通用名", "标准商品名", "标准剂型", "标准规格", 
                    "标准包装数量", "标准生产企业", "标准省份名称", "标准城市名称", "PACK_ID", "Sales", "Units", "PANEL", "DOI"]
        else:
            out_cols = ["project", "project_score", "Date", "ATC", "标准通用名", "标准商品名", "标准剂型", "标准规格", 
                    "标准包装数量", "标准生产企业", "标准省份名称", "标准城市名称", "PACK_ID", "Sales", "Units", "PANEL"]
    elif data_type == 'raw':
        out_cols = ["project", "project_score", "ID", "Raw_Hosp_Name", "PHA", "PHA医院名称" ,"Date", "ATC", "标准通用名", "标准商品名", 
                    "标准剂型", "标准规格", "标准包装数量", "标准生产企业", "标准省份名称", "标准城市名称", "PACK_ID", "Sales", "Units", 
                    "Units_Box"]
    
    # 列名转为大写
    df_max_filter_out = df_max_filter_out.select(out_cols).distinct()
    df_max_filter_out = df_max_filter_out.toDF(*[i.upper() for i in df_max_filter_out.columns])

    # %%
    # 输出提数结果
    df_max_filter_out = df_max_filter_out.repartition(1)
    df_max_filter_out.write.format("csv").option("header", "true") \
        .mode("overwrite").save(out_extract_data_path)

