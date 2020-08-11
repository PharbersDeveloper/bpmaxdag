# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
import pandas as pd
from phlogs.phlogs import phlogger
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func


def execute(max_path, project_name, model_month_right, max_month, year_missing, current_year, first_month, current_month, 
if_others, monthly_update, not_arrived_path, published_path, out_path, out_dir, need_test, if_add_data):
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data from s3") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "1") \
        .config("spark.executor.memory", "3g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .getOrCreate()

    access_key = "AKIAWPBDTVEAJ6CCFVCP"
    secret_key = "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"
    if access_key is not None:
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
        # spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
        spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.cn-northwest-1.amazonaws.com.cn")

    '''
    注意：杨森，月更新补数脚本特殊处理已经写脚本中
    '''
    
    phlogger.info('job3_data_adding')
    
    if if_add_data != "False" and if_add_data != "True":
        phlogger.error('wrong input: if_add_data, False or True') 
        raise ValueError('wrong input: if_add_data, False or True')
    
    if if_others == "True":
        out_dir = out_dir + "/others_box/"
    
    out_path_dir = out_path + "/" + project_name + '/' + out_dir

    # 输入
    product_mapping_out_path = out_path_dir + "/product_mapping_out"
    products_of_interest_path = max_path + "/" + project_name + "/poi.csv"
    if year_missing:
        year_missing = year_missing.replace(" ","").split(",")
    else:
        year_missing = []
    year_missing = [int(i) for i in year_missing]
    model_month_right = int(model_month_right)
    max_month = int(max_month)
    
    # 月更新相关参数
    if monthly_update != "False" and monthly_update != "True":
        phlogger.error('wrong input: monthly_update, False or True') 
        raise ValueError('wrong input: monthly_update, False or True')
    if monthly_update == "True":
        current_year = int(current_year)
        first_month = int(first_month)
        current_month = int(current_month)
        if not_arrived_path == "Empty":    
            not_arrived_path = max_path + "/Common_files/Not_arrived" + str(current_year*100 + current_month) + ".csv"
        if published_path == "Empty":
            published_right_path = max_path + "/Common_files/Published" + str(current_year) + ".csv"
            published_left_path = max_path + "/Common_files/Published" + str(current_year - 1) + ".csv"
        else:
            published_path  = published_path.replace(" ","").split(",")
            published_left_path = published_path[0]
            published_right_path = published_path[1]
    # not_arrived_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/Not_arrived202004.csv"
    # published_left_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/Published2019.csv"
    # published_right_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/Published2020.csv"

    # 输出
    price_path = out_path_dir + "/price"
    growth_rate_path = out_path_dir + "/growth_rate"
    adding_data_path =  out_path_dir + "/adding_data"
    raw_data_adding_path =  out_path_dir + "/raw_data_adding"
    new_hospital_path = out_path_dir + "/new_hospital"
    raw_data_adding_final_path =  out_path_dir + "/raw_data_adding_final"


    # =========== 数据检查 =============

    phlogger.info('数据检查-start')
    
    # csv文件检查
    length_error_dict = {}
    if monthly_update == "True":
        csv_path_list = [published_left_path, published_right_path, not_arrived_path]
        for eachpath in csv_path_list:
            eachfile = spark.read.csv(eachpath, header=True)
            ID_length = eachfile.withColumn("ID_length", func.length("ID")).select("ID_length").distinct().toPandas()["ID_length"].values.tolist()
            for i in ID_length:
                if i < 6:
                    length_error_dict[eachpath] = ID_length
        if length_error_dict:
            phlogger.error('ID length error: %s' % (length_error_dict))
            raise ValueError('ID length error: %s' % (length_error_dict))

    # 存储文件的缺失列
    misscols_dict = {}

    # product_mapping_out file
    raw_data = spark.read.parquet(product_mapping_out_path)
    colnames_raw_data = raw_data.columns
    misscols_dict.setdefault("product_mapping_out", [])

    colnamelist = ['min1', 'PHA', 'City', 'year_month', 'ID',  'Brand', 'Form',
    'Specifications', 'Pack_Number', 'Manufacturer', 'Molecule', 'Source', 
    'Sales', 'Units', 'Path', 'Sheet', 'BI_hospital_code', 'Province',
    'Month', 'Year', 'City_Tier_2010', 'min2', 'S_Molecule', 'std_route', "标准商品名"]
    #'Raw_Hosp_Name','ORG_Measure','Units_Box', 'Corp', 'Route'

    for each in colnamelist:
        if each not in colnames_raw_data:
            misscols_dict["product_mapping_out"].append(each)

    # 判断输入文件是否有缺失列
    misscols_dict_final = {}
    for eachfile in misscols_dict.keys():
        if len(misscols_dict[eachfile]) != 0:
            misscols_dict_final[eachfile] = misscols_dict[eachfile]
    # 如果有缺失列，则报错，停止运行
    if misscols_dict_final:
        phlogger.error('miss columns: %s' % (misscols_dict_final))
        raise ValueError('miss columns: %s' % (misscols_dict_final))

    phlogger.info('数据检查-Pass')

    # =========== 数据执行 =============
    phlogger.info('数据执行-start')

    # 数据读取
    raw_data = spark.read.parquet(product_mapping_out_path)
    raw_data.persist()
    products_of_interest = spark.read.csv(products_of_interest_path, header=True)
    products_of_interest = products_of_interest.toPandas()["poi"].values.tolist()

    # raw_data 处理
    raw_data = raw_data.withColumn("S_Molecule_for_gr",
                                   func.when(raw_data["标准商品名"].isin(products_of_interest), raw_data["标准商品名"]).
                                   otherwise(raw_data.S_Molecule))

    phlogger.info('1 价格计算')

    # 1 价格计算：cal_price 补数部分的数量需要用价格得出
    price = raw_data.groupBy("min2", "year_month", "City_Tier_2010") \
        .agg((func.sum("Sales") / func.sum("Units")).alias("Price"))
    price2 = raw_data.groupBy("min2", "year_month") \
        .agg((func.sum("Sales") / func.sum("Units")).alias("Price2"))
    price = price.join(price2, on=["min2", "year_month"], how="left")
    price = price.withColumn("Price", func.when(func.isnull(price.Price), price.Price2).
                             otherwise(price.Price))
    price = price.withColumn("Price", func.when(func.isnull(price.Price), func.lit(0)).
                             otherwise(price.Price)) \
        .drop("Price2")

    # 输出price
    price = price.repartition(2)
    price.write.format("parquet") \
        .mode("overwrite").save(price_path)

    phlogger.info("输出 price：".decode("utf-8") + price_path)

    # raw_data 处理
    if monthly_update == "False":
        raw_data = raw_data.where(raw_data.Year < ((model_month_right // 100) + 1))
        if project_name == "Sanofi" or project_name == "AZ":
            raw_data = raw_data.where((raw_data.Year > 2016) & (raw_data.Year < 2020))
    elif monthly_update == "True":
        if project_name == "Sanofi" or project_name == "AZ":
            raw_data = raw_data.where(raw_data.Year > 2016)

    phlogger.info('2 连续性计算')

    # 2 计算样本医院连续性: cal_continuity
    # 每个医院每年的月份数
    continuity = raw_data.select("Year", "Month", "PHA").distinct() \
        .groupBy("PHA", "Year").count()
    # 每个医院最大月份数，最小月份数
    continuity_whole_year = continuity.groupBy("PHA") \
        .agg(func.max("count").alias("MAX"), func.min("count").alias("MIN"))
    continuity = continuity.repartition(2, "PHA")

    years = continuity.select("Year").distinct().toPandas()["Year"].sort_values().values.tolist()
    # 数据长变宽
    continuity = continuity.groupBy("PHA").pivot("Year").agg(func.sum('count')).fillna(0)
    # 列名修改
    for eachyear in years:
        eachyear = str(eachyear)
        continuity = continuity.withColumn(eachyear, continuity[eachyear].cast(DoubleType())) \
            .withColumnRenamed(eachyear, "Year_" + eachyear)
    # year列求和
    # month_sum = con.Year_2018 + con.Year_2019
    month_sum = ""
    for i in continuity.columns[1:]:
        month_sum += ("continuity." + i + "+")
    month_sum = month_sum.strip('+')
    continuity = continuity.withColumn("total", eval(month_sum))
    # ['PHA', 'Year_2018', 'Year_2019', 'total', 'MAX', 'MIN']
    continuity = continuity.join(continuity_whole_year, on="PHA", how="left")

    # 3 计算样本分子增长率: cal_growth
    def calculate_growth(raw_data, max_month=12):
        # TODO: 完整年用完整年增长，不完整年用不完整年增长
        if max_month < 12:
            raw_data = raw_data.where(raw_data.Month <= max_month)

        # raw_data 处理
        growth_raw_data = raw_data.na.fill({"City_Tier_2010": 5.0})
        growth_raw_data = growth_raw_data.withColumn("CITYGROUP", growth_raw_data.City_Tier_2010)

        # 增长率计算过程
        growth_calculating = growth_raw_data.groupBy("S_Molecule_for_gr", "CITYGROUP", "Year") \
            .agg(func.sum(growth_raw_data.Sales).alias("value"))

        years = growth_calculating.select("Year").distinct().toPandas()["Year"].sort_values().values.tolist()
        years = [str(i) for i in years]
        years_name = ["Year_" + i for i in years]
        # 数据长变宽
        growth_calculating = growth_calculating.groupBy("S_Molecule_for_gr", "CITYGROUP").pivot("Year").agg(func.sum('value')).fillna(0)
        growth_calculating = growth_calculating.select(["S_Molecule_for_gr", "CITYGROUP"] + years)
        # 对year列名修改
        for i in range(0, len(years)):
            growth_calculating = growth_calculating.withColumnRenamed(years[i], years_name[i])

        # 计算得到年增长： add_gr_cols
        for i in range(0, len(years) - 1):
            growth_calculating = growth_calculating.withColumn("GR" + years[i][2:4] + years[i + 1][2:4],
                                                        growth_calculating[years_name[i + 1]] / growth_calculating[years_name[i]])
        growth_rate = growth_calculating     
        # 增长率的调整：modify_gr
        for y in [name for name in growth_rate.columns if name.startswith("GR")]:
            growth_rate = growth_rate.withColumn(y, func.when(func.isnull(growth_rate[y]) | (growth_rate[y] > 10) | (growth_rate[y] < 0.1), 1).
                                                 otherwise(growth_rate[y]))
        return growth_rate

    phlogger.info('3 增长率计算')
    # 执行函数 calculate_growth
    if monthly_update == "False":
        # AZ-Sanofi 要特殊处理
        if project_name != "Sanofi" and project_name != "AZ":
            growth_rate = calculate_growth(raw_data)
        else:
            year_missing_df = pd.DataFrame(year_missing, columns=["Year"])
            year_missing_df = spark.createDataFrame(year_missing_df)
            year_missing_df = year_missing_df.withColumn("Year", year_missing_df["Year"].cast(IntegerType()))
            # 完整年
            growth_rate_p1 = calculate_growth(raw_data.join(year_missing_df, on=["Year"], how="left_anti"))
            # 不完整年
            growth_rate_p2 = calculate_growth(raw_data.where(raw_data.Year.isin(year_missing + [y - 1 for y in year_missing] + [y + 1 for y in year_missing])), max_month)
    
            growth_rate = growth_rate_p1.select("S_Molecule_for_gr", "CITYGROUP") \
                .union(growth_rate_p2.select("S_Molecule_for_gr", "CITYGROUP")) \
                .distinct()
            growth_rate = growth_rate.join(
                growth_rate_p1.select(["S_Molecule_for_gr", "CITYGROUP"] + [name for name in growth_rate_p1.columns if name.startswith("GR")]),
                on=["S_Molecule_for_gr", "CITYGROUP"],
                how="left")
            growth_rate = growth_rate.join(
                growth_rate_p2.select(["S_Molecule_for_gr", "CITYGROUP"] + [name for name in growth_rate_p2.columns if name.startswith("GR")]),
                on=["S_Molecule_for_gr", "CITYGROUP"],
                how="left")
            
    elif monthly_update == "True":
        published_left = spark.read.csv(published_left_path, header=True)
        published_right = spark.read.csv(published_right_path, header=True)
        not_arrived =  spark.read.csv(not_arrived_path, header=True)
       
        for index, month in enumerate(range(first_month, current_month + 1)):
            
            raw_data_month = raw_data.where(raw_data.Month == month)
            
            if if_add_data == "False":
                growth_rate_month = calculate_growth(raw_data_month)
            else:
                # publish交集，去除当月未到
                month_hospital = published_left.intersect(published_right) \
                    .exceptAll(not_arrived.where(not_arrived.Date == current_year*100 + month).select("ID")) \
                    .toPandas()["ID"].tolist()
                growth_rate_month = calculate_growth(raw_data_month.where(raw_data_month.ID.isin(month_hospital)))
                # 标记是哪个月补数要用的growth_rate
                
            growth_rate_month = growth_rate_month.withColumn("month_for_monthly_add", func.lit(month))
            
            # 输出growth_rate结果
            if index == 0:
                # growth_rate = growth_rate_month
                growth_rate_month = growth_rate_month.repartition(1)
                growth_rate_month.write.format("parquet") \
                    .mode("overwrite").save(growth_rate_path)
            else:
                # growth_rate = growth_rate.union(growth_rate_month)
                growth_rate_month = growth_rate_month.repartition(1)
                growth_rate_month.write.format("parquet") \
                    .mode("append").save(growth_rate_path)
                    
            phlogger.info("输出 growth_rate：".decode("utf-8") + growth_rate_path)
                
    
    if monthly_update == "False":
        growth_rate = growth_rate.repartition(2)
        growth_rate.write.format("parquet") \
            .mode("overwrite").save(growth_rate_path)
        phlogger.info("输出 growth_rate：".decode("utf-8") + growth_rate_path)
    elif monthly_update == "True":
        growth_rate = spark.read.parquet(growth_rate_path)

    phlogger.info('数据执行-Finish')

    # =========== 数据验证 =============
    # 与原R流程运行的结果比较正确性: Sanofi与Sankyo测试通过
    if int(need_test) > 0:
        phlogger.info('数据验证-start')

        my_out = spark.read.parquet(raw_data_adding_final_path)
        
        if project_name == "Sanofi":
            R_out_path = "/common/projects/max/AZ_Sanofi/adding_data_new"
        elif project_name == "AZ":
            R_out_path = "/user/ywyuan/max/AZ/Rout/adding_data_new"
        elif project_name == "Sankyo":
            R_out_path = "/common/projects/max/Sankyo/adding_data_new"
        elif project_name == "Astellas":
            R_out_path = "/common/projects/max/Astellas/adding_data_new"
        elif project_name == "京新".decode("utf-8"):
            R_out_path = u"/common/projects/max/京新/adding_data_new"
        R_out = spark.read.parquet(R_out_path)

        # 检查内容：列缺失，列的类型，列的值
        for colname, coltype in R_out.dtypes:
            # 列是否缺失
            if colname not in my_out.columns:
                phlogger.warning ("miss columns:", colname)
            else:
                # 数据类型检查
                if my_out.select(colname).dtypes[0][1] != coltype:
                    phlogger.warning("different type columns: " + colname + ", " + my_out.select(colname).dtypes[0][1] + ", " + "right type: " + coltype)

                # 数值列的值检查
                if coltype == "double" or coltype == "int":
                    sum_my_out = my_out.groupBy().sum(colname).toPandas().iloc[0, 0]
                    sum_R = R_out.groupBy().sum(colname).toPandas().iloc[0, 0]
                    # phlogger.info(colname, sum_raw_data, sum_R)
                    if (sum_my_out - sum_R) != 0:
                        phlogger.warning("different value(sum) columns: " + colname + ", " + str(sum_my_out) + ", " + "right value: " + str(sum_R))

        phlogger.info('数据验证-Finish')

    # =========== return =============
    return growth_rate
