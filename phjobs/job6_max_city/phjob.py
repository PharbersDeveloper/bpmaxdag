# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from ph_logs.ph_logs import phlogger
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
import os


def execute(max_path, project_name, time_left, time_right, left_models, left_models_time_left, right_models, right_models_time_right,
all_models, if_others, out_path, out_dir, need_test, minimum_product_columns, minimum_product_sep, minimum_product_newname, if_two_source,
bedsize, hospital_level):
    os.environ["PYSPARK_PYTHON"] = "python3"
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data from s3") \
        .config("spark.driver.memory", "2g") \
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

    phlogger.info('job6_max_city')
    
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
    ID_Bedsize_path = max_path + "/Common_files/ID_Bedsize"
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
        for col in raw_data.columns:
            if col in ["数量（支/片）", "最小制剂单位数量", "total_units", "SALES_QTY"]:
                raw_data = raw_data.withColumnRenamed(col, "Units")
            if col in ["金额（元）", "金额", "sales_value__rmb_", "SALES_VALUE"]:
                raw_data = raw_data.withColumnRenamed(col, "Sales")
            if col in ["Yearmonth", "YM", "Date"]:
                raw_data = raw_data.withColumnRenamed(col, "year_month")
            if col in ["医院编码", "BI_Code", "HOSP_CODE"]:
                raw_data = raw_data.withColumnRenamed(col, "ID")
                
        raw_data = raw_data.withColumn("year_month", raw_data["year_month"].cast(IntegerType()))
        raw_data = raw_data.where((raw_data.year_month >=time_left) & (raw_data.year_month <=time_right))
        
        cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
        cpa_pha_mapping = cpa_pha_mapping.where(cpa_pha_mapping["推荐版本"] == 1) \
            .select("ID", "PHA").distinct()
            
        raw_data = raw_data.join(cpa_pha_mapping, on="ID", how="left")
        
        raw_data = deal_ID_length(raw_data)
        
        # job2: raw_data 处理，生成min1，用product_map 匹配获得min2（Prod_Name），同job2
        if project_name != "Mylan":
            raw_data = raw_data.withColumn("Brand", func.when(func.isnull(raw_data.Brand), raw_data.Molecule).
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
        if project_name == "Mylan":
            raw_data = raw_data.drop("tmp")
        else:
            if minimum_product_newname in raw_data.columns:
                raw_data = raw_data.drop(minimum_product_newname)
            raw_data = raw_data.withColumnRenamed("tmp", minimum_product_newname)
    
        # product_map
        product_map = spark.read.parquet(product_map_path)
        for col in product_map.columns:
            if col in ["标准通用名", "通用名_标准", "药品名称_标准", "S_Molecule_Name"]:
                product_map = product_map.withColumnRenamed(col, "通用名")
            if col in ["商品名_标准", "S_Product_Name"]:
                product_map = product_map.withColumnRenamed(col, "标准商品名")
            if col in ["标准途径"]:
                product_map = product_map.withColumnRenamed(col, "std_route")
            if col in ["min1_标准"]:
                product_map = product_map.withColumnRenamed(col, "min2")
        if "std_route" not in product_map.columns:
            product_map = product_map.withColumn("std_route", func.lit(''))	
            
        product_map_for_rawdata = product_map.select("min1", "min2", "通用名").distinct()
        
        raw_data = raw_data.join(product_map_for_rawdata, on="min1", how="left") \
            .drop("S_Molecule") \
            .withColumnRenamed("通用名", "S_Molecule")
            
            
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
    
    if hospital_level == "True":
        raw_data_city = raw_data \
            .groupBy("Province", "City", "Date", "Prod_Name", "PANEL", "DOI", "S_Molecule", "PHA") \
            .agg({"Sales":"sum", "Units":"sum"}) \
            .withColumnRenamed("sum(Sales)", "Predict_Sales") \
            .withColumnRenamed("sum(Units)", "Predict_Unit") \
            .withColumnRenamed("S_Molecule", "Molecule") 
    else:
        raw_data_city = raw_data \
            .groupBy("Province", "City", "Date", "Prod_Name", "PANEL", "DOI", "S_Molecule") \
            .agg({"Sales":"sum", "Units":"sum"}) \
            .withColumnRenamed("sum(Sales)", "Predict_Sales") \
            .withColumnRenamed("sum(Units)", "Predict_Unit") \
            .withColumnRenamed("S_Molecule", "Molecule") 
    
    # 2. max文件处理
    index = 0
    for market in all_models:
        # market 的 time_left 和 time_right 选择，默认为参数时间
        if market in left_models:
            time_left = left_models_time_left
        if market in right_models:
            time_right = right_models_time_right
        
        time_range = str(time_left) + '_' + str(time_right)
        
        if if_others:
            max_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + '_'  + market + "_hosp_level_box"
        else:
            max_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + '_' + market + "_hosp_level"
            
        max_result = spark.read.parquet(max_path)
        
        # max_result 筛选 BEDSIZE > 99， 且医院不在raw_data_PHA 中
        if bedsize == "True":
            max_result = max_result.where(max_result.BEDSIZE > 99)
        max_result = max_result.join(raw_data_PHA, on=["PHA", "Date"], how="left_anti")
        
        if hospital_level == "True":
            max_result = max_result \
                .groupBy("Province", "City", "Date", "Prod_Name", "PANEL", "Molecule", "PHA") \
                .agg({"Predict_Sales":"sum", "Predict_Unit":"sum"}) \
                .withColumnRenamed("sum(Predict_Sales)", "Predict_Sales") \
                .withColumnRenamed("sum(Predict_Unit)", "Predict_Unit")
        else:
            max_result = max_result \
                .groupBy("Province", "City", "Date", "Prod_Name", "PANEL", "Molecule") \
                .agg({"Predict_Sales":"sum", "Predict_Unit":"sum"}) \
                .withColumnRenamed("sum(Predict_Sales)", "Predict_Sales") \
                .withColumnRenamed("sum(Predict_Unit)", "Predict_Unit")
        
        max_result = max_result.withColumn("DOI", func.lit(market))
        
        
        if index ==0:
            # max_result_all = max_result
            max_result = max_result.repartition(1)
            max_result.write.format("parquet") \
                .mode("overwrite").save(max_result_city_tmp_path)
            
        else:
            # max_result_all = max_result_all.union(max_result)
            max_result = max_result.repartition(1)
            max_result.write.format("parquet") \
                .mode("append").save(max_result_city_tmp_path)
    
        index = index + 1
        
    
    max_result_all = spark.read.parquet(max_result_city_tmp_path)
    
    # 3. 合并raw_data 和 max文件处理
    if hospital_level == "True":
        raw_data_city = raw_data_city.select("PHA", "Province", "City", "Date", "Prod_Name", "Molecule", "PANEL", "DOI", "Predict_Sales", "Predict_Unit")
        max_result_all = max_result_all.select("PHA", "Province", "City", "Date", "Prod_Name", "Molecule", "PANEL", "DOI", "Predict_Sales", "Predict_Unit")
    else:
        raw_data_city = raw_data_city.select("Province", "City", "Date", "Prod_Name", "Molecule", "PANEL", "DOI", "Predict_Sales", "Predict_Unit")
        max_result_all = max_result_all.select("Province", "City", "Date", "Prod_Name", "Molecule", "PANEL", "DOI", "Predict_Sales", "Predict_Unit")
    
    max_result_city = max_result_all.union(raw_data_city)
    
    # 4. 合并后再进行一次group
    if hospital_level == "True":
        max_result_city = max_result_city \
            .groupBy("Province", "City", "Date", "Prod_Name", "PANEL", "Molecule", "PHA", "DOI") \
            .agg({"Predict_Sales":"sum", "Predict_Unit":"sum"}) \
            .withColumnRenamed("sum(Predict_Sales)", "Predict_Sales") \
            .withColumnRenamed("sum(Predict_Unit)", "Predict_Unit")
        max_result_city = max_result_city.select("PHA", "Province", "City", "Date", "Prod_Name", "Molecule", "PANEL", "DOI", "Predict_Sales", "Predict_Unit")
    else:
        max_result_city = max_result_city \
            .groupBy("Province", "City", "Date", "Prod_Name", "PANEL", "Molecule", "DOI") \
            .agg({"Predict_Sales":"sum", "Predict_Unit":"sum"}) \
            .withColumnRenamed("sum(Predict_Sales)", "Predict_Sales") \
            .withColumnRenamed("sum(Predict_Unit)", "Predict_Unit")
        max_result_city = max_result_city.select("Province", "City", "Date", "Prod_Name", "Molecule", "PANEL", "DOI", "Predict_Sales", "Predict_Unit")
    
    # hospital_level 的只输出csv
    if hospital_level == "False" and bedsize == "True":     
        max_result_city = max_result_city.repartition(2)
        max_result_city.write.format("parquet") \
            .mode("overwrite").save(max_result_city_path)
    
    max_result_city = max_result_city.repartition(1)
    max_result_city.write.format("csv").option("header", "true") \
        .mode("overwrite").save(max_result_city_csv_path)
        
