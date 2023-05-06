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
    time_left = kwargs['time_left']
    time_right = kwargs['time_right']
    left_models = kwargs['left_models']
    left_models_time_left = kwargs['left_models_time_left']
    right_models = kwargs['right_models']
    right_models_time_right = kwargs['right_models_time_right']
    all_models = kwargs['all_models']
    if_others = kwargs['if_others']
    out_path = kwargs['out_path']
    out_dir = kwargs['out_dir']
    need_test = kwargs['need_test']
    minimum_product_columns = kwargs['minimum_product_columns']
    minimum_product_sep = kwargs['minimum_product_sep']
    minimum_product_newname = kwargs['minimum_product_newname']
    if_two_source = kwargs['if_two_source']
    hospital_level = kwargs['hospital_level']
    bedsize = kwargs['bedsize']
    id_bedsize_path = kwargs['id_bedsize_path']
    for_nh_model = kwargs['for_nh_model']
    ### input args ###
    
    ### output args ###
    c = kwargs['c']
    d = kwargs['d']
    ### output args ###

    from pyspark.sql import SparkSession
    from pyspark.sql.types import StringType, IntegerType, DoubleType
    from pyspark.sql import functions as func
    from pyspark.sql.functions import col
    import boto3
    import os   
    import pandas as pd    # %%
    
    # project_name = "Gilead"
    # time_left = "202001"
    # time_right = "202012"
    # all_models = "乙肝"
    # out_dir = "202012" 
    # if_two_source = "True"
    

    # %%
    # 输入
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
    
    if left_models != "Empty":
        left_models = left_models.replace(", ",",").split(",")
    else:
        left_models = []
    
    if right_models != "Empty":
        right_models = right_models.replace(", ",",").split(",")
    else:
        right_models = []
    
    if left_models_time_left == "Empty":
        left_models_time_left = 0
    if right_models_time_right == "Empty":
        right_models_time_right = 0
    
    if all_models != "Empty":
        all_models = all_models.replace(", ",",").split(",")
    else:
        all_models = []
    
    time_left = int(time_left)
    time_right = int(time_right)
    
    province_city_mapping_path = max_path + "/" + project_name + '/province_city_mapping'
    hospital_ot_path = max_path + "/" + project_name + '/hospital_ot.csv'
    market_mapping_path = max_path + "/" + project_name + '/mkt_mapping'
    cpa_pha_mapping_path = max_path + "/" + project_name + "/cpa_pha_mapping"
    if id_bedsize_path == 'Empty':
        ID_Bedsize_path = max_path + "/Common_files/ID_Bedsize"
    else:
        ID_Bedsize_path = id_bedsize_path
    
    cpa_pha_mapping_common_path = max_path + "/Common_files/cpa_pha_mapping"
    
    if if_others == True:
        out_dir = out_dir + "/others_box/"
    
    out_path_dir = out_path + "/" + project_name + '/' + out_dir
    product_map_path = out_path_dir + "/prod_mapping"
    if if_two_source == "False":
        raw_data_std_path = out_path_dir + "/product_mapping_out"
    else:
        raw_data_std_path = out_path_dir + "/raw_data_std"
    
    # 输出
    time_range = str(time_left) + '_' + str(time_right)
    tmp_path = out_path_dir + "/MAX_result/tmp"
    if hospital_level == "True" and bedsize == "False":
        max_result_city_csv_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + "_hospital_level_nobed.csv"
        max_result_city_tmp_path = out_path_dir + "/MAX_result/tmp_hospital_nobed_"+ time_range
    elif hospital_level == "True":
        max_result_city_csv_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + "_hospital_level.csv"
        max_result_city_tmp_path = out_path_dir + "/MAX_result/tmp_hospital_"+ time_range
    elif hospital_level == "False" and bedsize == "False":
        max_result_city_csv_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + "_city_level_nobed.csv"
        max_result_city_tmp_path = out_path_dir + "/MAX_result/tmp_city_nobed_"+ time_range
    else:
        max_result_city_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + "_city_level"
        max_result_city_csv_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + "_city_level.csv"
        max_result_city_tmp_path = out_path_dir + "/MAX_result/tmp_city_"+ time_range
        
    if for_nh_model == 'True':
        NH_in_old_universe_path = max_path + "/Common_files/NH_in_old_universe"
        df_NH_in_old_universe = spark.read.parquet(NH_in_old_universe_path).select('Panel_ID').withColumnRenamed('Panel_ID','PHA').withColumn("NH_in_old_universe",func.lit(1)).distinct()

    # %%
    # =========== 数据执行 =============
    '''
    合并raw_data 和 max 结果
    '''
    def deal_ID_length(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字，其他还有包含字母的ID
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df

    raw_data = spark.read.parquet(raw_data_std_path)

    if if_two_source == "False":
        raw_data = raw_data.where((raw_data.year_month >=time_left) & (raw_data.year_month <=time_right))

    else:
        # job1: raw_data 处理，匹配PHA，部分job1
        for i in raw_data.columns:
            if i in ["数量（支/片）", "最小制剂单位数量", "total_units", "SALES_QTY"]:
                raw_data = raw_data.withColumnRenamed(i, "Units")
            if i in ["金额（元）", "金额", "sales_value__rmb_", "SALES_VALUE"]:
                raw_data = raw_data.withColumnRenamed(i, "Sales")
            if i in ["Yearmonth", "YM", "Date"]:
                raw_data = raw_data.withColumnRenamed(i, "year_month")
            if i in ["医院编码", "BI_Code", "HOSP_CODE"]:
                raw_data = raw_data.withColumnRenamed(i, "ID")

        raw_data = raw_data.withColumn("year_month", raw_data["year_month"].cast(IntegerType()))
        raw_data = raw_data.where((raw_data.year_month >=time_left) & (raw_data.year_month <=time_right))

        cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
        cpa_pha_mapping = cpa_pha_mapping.where(cpa_pha_mapping["推荐版本"] == 1) \
            .select("ID", "PHA").distinct()
        cpa_pha_mapping = deal_ID_length(cpa_pha_mapping)

        raw_data = deal_ID_length(raw_data)    
        raw_data = raw_data.join(cpa_pha_mapping, on="ID", how="left")

        # job2: raw_data 处理，生成min1，用product_map 匹配获得min2（Prod_Name），同job2
        # if project_name != "Mylan":
        raw_data = raw_data.withColumn("Brand", func.when((raw_data.Brand.isNull()) | (raw_data.Brand == 'NA'), raw_data.Molecule).
                                       otherwise(raw_data.Brand))

        for colname, coltype in raw_data.dtypes:
            if coltype == "logical":
                raw_data = raw_data.withColumn(colname, raw_data[colname].cast(StringType()))

        raw_data = raw_data.withColumn("tmp", func.when(func.isnull(raw_data[minimum_product_columns[0]]), func.lit("NA")).
                                       otherwise(raw_data[minimum_product_columns[0]]))

        for col in minimum_product_columns[1:]:
            raw_data = raw_data.withColumn(col, raw_data[col].cast(StringType()))
            raw_data = raw_data.withColumn("tmp", func.concat(
                raw_data["tmp"],
                func.lit(minimum_product_sep),
                func.when(func.isnull(raw_data[col]), func.lit("NA")).otherwise(raw_data[col])))

        # Mylan不重新生成minimum_product_newname: min1，其他项目生成min1
        # if project_name == "Mylan":
        #     raw_data = raw_data.drop("tmp")
        # else:
        if minimum_product_newname in raw_data.columns:
            raw_data = raw_data.drop(minimum_product_newname)
        raw_data = raw_data.withColumnRenamed("tmp", minimum_product_newname)

        # product_map
        product_map = spark.read.parquet(product_map_path)
        for i in product_map.columns:
            if i in ["标准通用名", "通用名_标准", "药品名称_标准", "S_Molecule_Name"]:
                product_map = product_map.withColumnRenamed(i, "通用名")
            if i in ["商品名_标准", "S_Product_Name"]:
                product_map = product_map.withColumnRenamed(i, "标准商品名")
            if i in ["标准途径"]:
                product_map = product_map.withColumnRenamed(i, "std_route")
            if i in ["min1_标准"]:
                product_map = product_map.withColumnRenamed(i, "min2")
        if "std_route" not in product_map.columns:
            product_map = product_map.withColumn("std_route", func.lit(''))	

        product_map_for_rawdata = product_map.select("min1", "min2", "通用名").distinct()

        raw_data = raw_data.join(product_map_for_rawdata, on="min1", how="left") \
            .drop("S_Molecule") \
            .withColumnRenamed("通用名", "S_Molecule")

    raw_data = deal_ID_length(raw_data)

    # 匹配市场名
    market_mapping = spark.read.parquet(market_mapping_path)
    market_mapping = market_mapping.withColumnRenamed("标准通用名", "通用名") \
                .withColumnRenamed("model", "mkt") \
                .select("mkt", "通用名").distinct()
    raw_data = raw_data.join(market_mapping, raw_data["S_Molecule"] == market_mapping["通用名"], how="left")


    # 列重命名
    raw_data = raw_data.withColumnRenamed("mkt", "DOI") \
                .withColumnRenamed("min2", "Prod_Name") \
                .withColumnRenamed("year_month", "Date") \
                .select("ID", "Date", "Prod_Name", "Sales", "Units", "DOI", "PHA", "S_Molecule")

    # 匹配通用cpa_city
    province_city_mapping = spark.read.parquet(province_city_mapping_path)
    province_city_mapping = province_city_mapping.distinct()
    province_city_mapping = deal_ID_length(province_city_mapping)

    raw_data = raw_data.join(province_city_mapping, on="ID", how="left") \
            .withColumn("PANEL", func.lit(1))

    # 删除医院
    # hospital_ot = spark.read.csv(hospital_ot_path, header=True)
    # raw_data = raw_data.join(hospital_ot, on="ID", how="left_anti")

    # raw_data PHA是空的重新匹配
    cpa_pha_mapping_common = spark.read.parquet(cpa_pha_mapping_common_path)
    cpa_pha_mapping_common = cpa_pha_mapping_common.where(cpa_pha_mapping_common["推荐版本"] == 1) \
            .withColumnRenamed("PHA", "PHA_common") \
            .select("ID", "PHA_common").distinct()
    cpa_pha_mapping_common = deal_ID_length(cpa_pha_mapping_common)

    raw_data = raw_data.join(cpa_pha_mapping_common, on="ID", how="left")
    raw_data = raw_data.withColumn("PHA", func.when(raw_data.PHA.isNull(), raw_data.PHA_common).otherwise(raw_data.PHA)) \
                    .drop("PHA_common")

    # raw_data 医院列表
    raw_data_PHA = raw_data.select("PHA", "Date").distinct()

    # ID_Bedsize 匹配
    ID_Bedsize = spark.read.parquet(ID_Bedsize_path)
    ID_Bedsize = deal_ID_length(ID_Bedsize)

    raw_data = raw_data.join(ID_Bedsize, on="ID", how="left")

    # all_models 筛选
    if raw_data.select("DOI").dtypes[0][1] == "double":
        raw_data = raw_data.withColumn("DOI", raw_data["DOI"].cast(IntegerType()))
    raw_data = raw_data.where(raw_data.DOI.isin(all_models))

    # 计算
    if project_name != "Janssen":
        if bedsize == "True":
            raw_data = raw_data.where(raw_data.Bedsize > 99)

    if for_nh_model == 'True':  
        raw_date_final = raw_data.join(df_NH_in_old_universe, on='PHA', how='left') \
                    .withColumn("NH_in_old_universe", func.when(col("NH_in_old_universe").isNull(), 0).otherwise(col("NH_in_old_universe"))) \
                    .withColumnRenamed("S_Molecule", "Molecule") 
    else:
        raw_date_final = raw_data.withColumnRenamed("S_Molecule", "Molecule") 

    if hospital_level == "True":
        if for_nh_model == 'True':
            groupby_list=["Province", "City", "Date", "Prod_Name", "PANEL", "DOI", "Molecule", "PHA", "NH_in_old_universe"]
        else:
            groupby_list=["Province", "City", "Date", "Prod_Name", "PANEL", "DOI", "Molecule", "PHA"]
    else:
        if for_nh_model == 'True':
            groupby_list=["Province", "City", "Date", "Prod_Name", "PANEL", "DOI", "Molecule", "NH_in_old_universe"]
        else:
            groupby_list=["Province", "City", "Date", "Prod_Name", "PANEL", "DOI", "Molecule"]

    raw_data_city = raw_date_final \
        .groupBy(groupby_list) \
        .agg({"Sales":"sum", "Units":"sum"}) \
        .withColumnRenamed("sum(Sales)", "Predict_Sales") \
        .withColumnRenamed("sum(Units)", "Predict_Unit")

    # %%
    # 2. max文件处理
    index = 0
    for market in all_models:
        # market 的 time_left 和 time_right 选择，默认为参数时间
        if market in left_models:
            time_left_1 = left_models_time_left
        else:
            time_left_1 = time_left
        if market in right_models:
            time_right_1 = right_models_time_right
        else:
            time_right_1 = time_right

        time_range = str(time_left_1) + '_' + str(time_right_1)

        if if_others:
            max_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + '_'  + market + "_hosp_level_box"
        else:
            max_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + '_' + market + "_hosp_level"

        max_result = spark.read.parquet(max_path)

        # max_result 筛选 BEDSIZE > 99， 且医院不在raw_data_PHA 中
        if bedsize == "True":
            max_result = max_result.where(max_result.BEDSIZE > 99)

        max_result = max_result.join(raw_data_PHA, on=["PHA", "Date"], how="left_anti")
        max_result = max_result.withColumn("DOI", func.lit(market))

        if for_nh_model == 'True':  
            max_result_final = max_result.join(df_NH_in_old_universe, on='PHA', how='left')\
                    .withColumn("NH_in_old_universe", func.when(col("NH_in_old_universe").isNull(), 0).otherwise(col("NH_in_old_universe")))
        else:
            max_result_final = max_result

        max_result_out = max_result_final \
            .groupBy(groupby_list) \
            .agg({"Predict_Sales":"sum", "Predict_Unit":"sum"}) \
            .withColumnRenamed("sum(Predict_Sales)", "Predict_Sales") \
            .withColumnRenamed("sum(Predict_Unit)", "Predict_Unit")

        if index ==0:
            # max_result_all = max_result_out
            # max_result_out = max_result_out.repartition(1)
            max_result_out.write.format("parquet") \
                .mode("overwrite").save(max_result_city_tmp_path)

        else:
            # max_result_all = max_result_all.union(max_result_out)
            # max_result_out = max_result_out.repartition(1)
            max_result_out.write.format("parquet") \
                .mode("append").save(max_result_city_tmp_path)

        index = index + 1

    max_result_all = spark.read.parquet(max_result_city_tmp_path)

    # %%
    # 3. 合并raw_data 和 max文件处理
    max_result_city_all = max_result_all.union(raw_data_city)

    # 4. 合并后再进行一次group
    max_result_city = max_result_city_all \
        .groupBy(groupby_list) \
        .agg({"Predict_Sales":"sum", "Predict_Unit":"sum"}) \
        .withColumnRenamed("sum(Predict_Sales)", "Predict_Sales") \
        .withColumnRenamed("sum(Predict_Unit)", "Predict_Unit")

    # %%
    # 5.输出判断是否已有 max_result_city_path 结果
    '''
    如果已经存在 max_result_city_path 则用新的结果对已有结果进行DOI替换和补充
    '''
    if hospital_level == "False" and bedsize == "True":
        file_name = max_result_city_path.replace('//', '/').split('s3:/ph-max-auto/')[1]
    else:
        file_name = max_result_city_csv_path.replace('//', '/').split('s3:/ph-max-auto/')[1]

    s3 = boto3.resource('s3', region_name='cn-northwest-1',
                            aws_access_key_id="AKIAWPBDTVEAEU44ZAGT",
                            aws_secret_access_key="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")
    bucket = s3.Bucket('ph-max-auto')
    judge = 0
    for obj in bucket.objects.filter(Prefix = file_name):
        path, filename = os.path.split(obj.key)  
        if path == file_name:
            judge += 1

    if judge > 0:
        if hospital_level == "False" and bedsize == "True":
            old_max_out = spark.read.parquet(max_result_city_path)
        else:
            old_max_out = spark.read.csv(max_result_city_csv_path, header=True)
        # 当期数据包含的市场
        new_markets = max_result_city.select('DOI').distinct()
        # 去掉已有数据中重复市场
        old_max_out_keep = old_max_out.join(new_markets, on='DOI', how='left_anti')   
        # old_max_out_keep = old_max_out.where(~old_max_out['DOI'].isin(new_markets))    
        max_result_city_final = max_result_city.union(old_max_out_keep.select(max_result_city.columns))
        # 中间文件读写一下
        # max_result_city_final = max_result_city_final.repartition(2)
        max_result_city_final.write.format("parquet") \
                            .mode("overwrite").save(tmp_path)
        max_result_city_final = spark.read.parquet(tmp_path)   
    else:
        max_result_city_final = max_result_city

    # %%
    # max_result_city_final.groupby('PANEL').agg(func.sum('Predict_Sales')).show()

    # %%
    # hospital_level 的只输出csv
    if hospital_level == "False" and bedsize == "True":     
        # max_result_city_final = max_result_city_final.repartition(2)
        max_result_city_final.write.format("parquet") \
            .mode("overwrite").save(max_result_city_path)

    if hospital_level == 'True':
        max_result_city_final.write.format("csv").option("header", "true") \
            .mode("overwrite").partitionBy("DOI", "Date").save(max_result_city_csv_path)
    else: 
        max_result_city_final = max_result_city_final.repartition(1)
        max_result_city_final.write.format("csv").option("header", "true") \
            .mode("overwrite").save(max_result_city_csv_path)
	
    if for_nh_model == 'True':
        max_result_city_final.where(col('NH_in_old_universe')==1).write.format("parquet") \
            .mode("overwrite").save(f"{max_result_city_path}_1for_factor")



