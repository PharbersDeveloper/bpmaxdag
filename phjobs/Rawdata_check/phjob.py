# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""
from ph_logs.ph_logs import phlogger
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
import os
from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, greatest, least
import time

def execute(max_path, project_name, outdir, minimum_product_sep, minimum_product_columns, current_year, current_month, 
three, twelve, test):
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
    
    # 输入
    current_year = int(current_year)
    current_month = int(current_month)
    three = int(three)
    twelve = int(twelve)
    
    minimum_product_columns = minimum_product_columns.replace(" ","").split(",")
    product_map_path = max_path + '/' + project_name + '/' + outdir + '/prod_mapping'
    if test == 'True':
        raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/raw_data'
    else:
        raw_data_path = max_path + '/' + project_name + '/' + outdir + '/raw_data'
    raw_data_check_path = max_path + '/' + project_name + '/' + outdir + '/raw_data_check/'
    
    # 输出
    check_result_path = raw_data_check_path + '/check_result.csv'
    check_1_path = raw_data_check_path + '/check_1_每个月产品个数.csv'
    check_2_path = raw_data_check_path + '/check_2_各产品历史月份销量.csv'
    check_3_path = raw_data_check_path + '/check_3_历史医院个数.csv'
    check_5_path = raw_data_check_path + '/check_5_最近12期每家医院每个月的金额规模.csv'
    check_8_path = raw_data_check_path + '/check_8_每个医院每个月产品个数.csv'
    check_9_1_path = raw_data_check_path + '/check_9_1_所有产品每个月金额.csv'
    check_9_2_path = raw_data_check_path + '/check_9_2_所有产品每个月份额.csv'
    check_9_3_path = raw_data_check_path + '/check_9_3_所有产品每个月排名.csv'
    check_10_path = raw_data_check_path + '/check_10_在售产品医院个数.csv'
    check_11_path = raw_data_check_path + '/check_11_全部医院历史贡献率等级.csv'
    tmp_1_path  = raw_data_check_path + '/tmp_1'
    tmp_2_path  = raw_data_check_path + '/tmp_2'
    
    # ================
    
    MTH = current_year*100 + current_month
    PREMTH = MTH - 1
    
    
    # 当前月的前3个月
    if three > (current_month - 1):
    	diff = three - current_month
    	RQMTH = [i for i in range((current_year - 1)*100 +12 - diff , (current_year - 1)*100 + 12 + 1)] + [i for i in range(MTH - current_month + 1 , MTH)]
    else:
    	RQMTH = [i for i in range(MTH - current_month + 1 , MTH)][-three:]
    
    # 当前月的前12个月
    if twelve > (current_month - 1):
    	diff = twelve - current_month
    	mat_month = [i for i in range((current_year - 1)*100 + 12 - diff , (current_year - 1)*100 + 12 + 1)] + [i for i in range(MTH - current_month + 1 , MTH)]
    else:
    	mat_month = [i for i in range(MTH - current_month + 1 , MTH)][-twelve:]
    
    
    # ================= 数据执行 ==================	
    Raw_data = spark.read.parquet(raw_data_path)
    Raw_data = Raw_data.withColumn('Date', Raw_data['Date'].cast(IntegerType())) \
                    .withColumn('Units', Raw_data['Units'].cast(DoubleType())) \
                    .withColumn('Sales', Raw_data['Sales'].cast(DoubleType()))
    
    # 生成min1
    Raw_data = Raw_data.withColumn('Brand_bak', Raw_data.Brand)
    Raw_data = Raw_data.withColumn('Brand', func.when((Raw_data.Brand.isNull()) | (Raw_data.Brand == 'NA'), Raw_data.Molecule).
                                                otherwise(Raw_data.Brand))
    Raw_data = Raw_data.withColumn("min1", func.when(Raw_data[minimum_product_columns[0]].isNull(), func.lit("NA")).
                                       otherwise(Raw_data[minimum_product_columns[0]]))
    for col in minimum_product_columns[1:]:
        Raw_data = Raw_data.withColumn(col, Raw_data[col].cast(StringType()))
        Raw_data = Raw_data.withColumn("min1", func.concat(
            Raw_data["min1"],
            func.lit(minimum_product_sep),
            func.when(func.isnull(Raw_data[col]), func.lit("NA")).otherwise(Raw_data[col])))
    Raw_data = Raw_data.withColumn('Brand', Raw_data.Brand_bak).drop('Brand_bak')
    
    # 产品匹配表处理 
    product_map = spark.read.parquet(product_map_path)
    # a. 列名清洗统一
    if project_name == "Sanofi" or project_name == "AZ":
        product_map = product_map.withColumnRenamed(product_map.columns[21], "pfc")
    for col in product_map.columns:
        if col in ["标准通用名", "通用名_标准", "药品名称_标准", "S_Molecule_Name"]:
            product_map = product_map.withColumnRenamed(col, "通用名")
        if col in ["min1_标准"]:
            product_map = product_map.withColumnRenamed(col, "min2")
        if col in ["packcode", "Pack_ID", "Pack_Id", "PackID", "packid"]:
            product_map = product_map.withColumnRenamed(col, "pfc")
        if col in ["商品名_标准", "S_Product_Name"]:
            product_map = product_map.withColumnRenamed(col, "标准商品名")
        if col in ["剂型_标准", "Form_std", "S_Dosage"]:
            product_map = product_map.withColumnRenamed(col, "标准剂型")
        if col in ["规格_标准", "Specifications_std", "药品规格_标准", "S_Pack"]:
            product_map = product_map.withColumnRenamed(col, "标准规格")
        if col in ["包装数量2", "包装数量_标准", "Pack_Number_std", "S_PackNumber", "最小包装数量"]:
            product_map = product_map.withColumnRenamed(col, "标准包装数量")
        if col in ["标准企业", "生产企业_标准", "Manufacturer_std", "S_CORPORATION", "标准生产厂家"]:
            product_map = product_map.withColumnRenamed(col, "标准生产企业")
    if project_name == "Janssen" or project_name == "NHWA":
        if "标准剂型" not in product_map.columns:
            product_map = product_map.withColumnRenamed("剂型", "标准剂型")
        if "标准规格" not in product_map.columns:
            product_map = product_map.withColumnRenamed("规格", "标准规格")
        if "标准生产企业" not in product_map.columns:
            product_map = product_map.withColumnRenamed("生产企业", "标准生产企业")
        if "标准包装数量" not in product_map.columns:
            product_map = product_map.withColumnRenamed("包装数量", "标准包装数量")
            
    # b. 选取需要的列
    product_map = product_map \
                    .select("min1", "min2", "pfc", "通用名", "标准商品名", "标准剂型", "标准规格", "标准包装数量", "标准生产企业") \
                    .distinct() \
                    .withColumnRenamed("标准商品名", "商品名") \
                    .withColumnRenamed("标准剂型", "剂型") \
                    .withColumnRenamed("标准规格", "规格") \
                    .withColumnRenamed("标准包装数量", "包装数量") \
                    .withColumnRenamed("标准生产企业", "生产企业") \
                    .withColumnRenamed("pfc", "Pack_ID")
    
    
    # 匹配产品匹配表，标准化min2通用名商品名
    Raw_data_1 = Raw_data.join(product_map.select('min1','min2','通用名','商品名','Pack_ID').dropDuplicates(['min1']), on='min1', how='left')
    Raw_data_1 = Raw_data_1.groupby('ID', 'Date', 'min2', '通用名','商品名','Pack_ID') \
                            .agg(func.sum('Sales').alias('Sales'), func.sum('Units').alias('Units')) \
                            .withColumnRenamed('min2', 'Prod_Name')
    
    #========== check_1 ==========
    
    # 每个月产品个数(min2)
    check_1 = Raw_data_1.select('Date', 'Prod_Name').distinct() \
                        .groupby('Date').count() \
                        .withColumnRenamed('count', '每月产品个数_min2') \
                        .orderBy('Date').persist()
    
    ### 判断产品个数与上月相比是否超过 8%
    MTH_product_num = check_1.where(check_1.Date == MTH).toPandas()['每月产品个数_min2'][0]
    PREMTH_product_num = check_1.where(check_1.Date == PREMTH).toPandas()['每月产品个数_min2'][0]
    check_result_1 = (MTH_product_num/PREMTH_product_num < 0.08)
    
    check_1 = check_1.repartition(1)
    check_1.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_1_path)
        
    #========== check_2 ==========
    
    # 各产品历史月份销量
    check_2 = Raw_data_1.groupby('Date', 'Prod_Name').agg(func.sum('Sales').alias('Sales'))
    check_2 = check_2.groupBy("Prod_Name").pivot("Date").agg(func.sum('Sales')).persist()
    
    ### 判断缺失产品是否在上个月销售金额超过 2%
    MTH_product_Sales = check_2.where(check_2[str(MTH)].isNull()).groupBy().agg(func.sum(str(PREMTH)).alias('sum')).toPandas()['sum'][0]
    PREMTH_product_Sales = check_2.groupBy().agg(func.sum(str(PREMTH)).alias('sum')).toPandas()['sum'][0]
    check_result_2 = (MTH_product_Sales/PREMTH_product_Sales < 0.08)
    
    check_2 = check_2.repartition(1)
    check_2.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_2_path)
        
    #========== check_3 ==========
    
    # 历史医院个数
    check_3 = Raw_data.select('Date', 'ID').distinct() \
                    .groupBy('Date').count() \
                    .withColumnRenamed('count', '医院个数') \
                    .orderBy('Date').persist()
    
    ### 判断历史医院个数是否超过1%                
    MTH_hospital_num = check_3.where(check_3.Date == MTH).toPandas()['医院个数'][0]
    PREMTH_hospital_num = check_3.where(check_3.Date == PREMTH).toPandas()['医院个数'][0]
    check_result_3 = (MTH_hospital_num/PREMTH_hospital_num -1 < 0.01)
    
    check_3 = check_3.repartition(1)
    check_3.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_3_path)
        
    
    
    # 最近12期每家医院每个月的销量规模
    check_5_1 = Raw_data.where(Raw_data.Date > (current_year - 1)*100 + current_month - 1) \
                        .groupBy('ID', 'Date').agg(func.sum('Sales').alias('Sales'))
    check_5_1 = check_5_1.groupBy("ID").pivot("Date").agg(func.sum('Sales')).persist() \
                        .orderBy('ID').persist()
                        
    ### 检查当月缺失医院在上个月的销售额占比
    MTH_hospital_Sales = check_5_1.where(check_5_1[str(MTH)].isNull()).groupBy().agg(func.sum(str(PREMTH)).alias('sum')).toPandas()['sum'][0]
    PREMTH_hospital_Sales = check_5_1.groupBy().agg(func.sum(str(PREMTH)).alias('sum')).toPandas()['sum'][0]
    check_result_5 = (MTH_hospital_Sales/PREMTH_hospital_Sales < 0.01)
    
    # 每家医院的月销金额在最近12期的误差范围内（mean+-1.96std），范围内的医院数量占比大于95%；
    check_5_2 = Raw_data.where((Raw_data.Date > (current_year-1)*100+current_month-1 ) & (Raw_data.Date < current_year*100+current_month)) \
                        .groupBy('ID', 'Date').agg(func.sum('Sales').alias('Sales')) \
                        .groupBy('ID').agg(func.mean('Sales').alias('Mean_Sales'), func.stddev('Sales').alias('Sd_Sales')).persist()
                        
    check_5_2 = check_5_2.join(Raw_data.where(Raw_data.Date == current_year*100+current_month).groupBy('ID').agg(func.sum('Sales').alias('Sales_newmonth')), 
                                            on='ID', how='left').persist()
    check_5_2 = check_5_2.withColumn('Check', func.when(check_5_2.Sales_newmonth < check_5_2.Mean_Sales-1.96*check_5_2.Sd_Sales, func.lit('F')) \
                                                .otherwise(func.when(check_5_2.Sales_newmonth > check_5_2.Mean_Sales+1.96*check_5_2.Sd_Sales, func.lit('F')) \
                                                                .otherwise(func.lit('T'))))
    check_5_2 = check_5_2.withColumn('Check', func.when(func.isnan(check_5_2.Mean_Sales) | func.isnan(check_5_2.Sd_Sales) | check_5_2.Sales_newmonth.isNull(), func.lit(None)) \
                                                    .otherwise(check_5_2.Check))                            
    
    check_5 = check_5_1.join(check_5_2, on='ID', how='left').orderBy('ID').persist()
    
    check_5 = check_5.repartition(1)
    check_5.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_5_path)
        
    #========== check_6 ==========
    
    # 最近12期每家医院每个月的销量规模
    check_6_1 = Raw_data.where(Raw_data.Date > (current_year-1)*100+current_month-1) \
                        .groupby('ID', 'Date').agg(func.sum('Units').alias('Units')) \
                        .groupBy("ID").pivot("Date").agg(func.sum('Units')).persist()
                        
    # 每家医院的月销数量在最近12期的误差范围内（mean+-1.96std），范围内的医院数量占比大于95%；
    check_6_2 = Raw_data.where((Raw_data.Date > (current_year-1)*100+current_month-1 ) & (Raw_data.Date < current_year*100+current_month)) \
                        .groupBy('ID', 'Date').agg(func.sum('Units').alias('Units')) \
                        .groupBy('ID').agg(func.mean('Units').alias('Mean_Units'), func.stddev('Units').alias('Sd_Units'))
    check_6_2 = check_6_2.join(Raw_data.where(Raw_data.Date == current_year*100+current_month).groupBy('ID').agg(func.sum('Units').alias('Units_newmonth')), 
                                            on='ID', how='left').persist()
    check_6_2 = check_6_2.withColumn('Check', func.when(check_6_2.Units_newmonth < check_6_2.Mean_Units-1.96*check_6_2.Sd_Units, func.lit('F')) \
                                                .otherwise(func.when(check_6_2.Units_newmonth > check_6_2.Mean_Units+1.96*check_6_2.Sd_Units, func.lit('F')) \
                                                                .otherwise(func.lit('T'))))
    check_6_2 = check_6_2.withColumn('Check', func.when(func.isnan(check_6_2.Mean_Units) | func.isnan(check_6_2.Sd_Units) | check_6_2.Units_newmonth.isNull(), func.lit(None)) \
                                                    .otherwise(check_6_2.Check)) 
                                                    
    check_6 = check_6_1.join(check_6_2, on='ID', how='left').orderBy('ID')
    
    #========== check_7 ==========
    
    # 最近12期每家医院每个月每个产品(Packid)的平均价格
    check_7_1 = Raw_data_1.where(Raw_data_1.Date > (current_year-1)*100+current_month-1) \
                        .groupBy('ID', 'Date', '通用名','商品名','Pack_ID') \
                        .agg(func.sum('Sales').alias('Sales'), func.sum('Units').alias('Units')).persist()
    check_7_1 = check_7_1.withColumn('Price', check_7_1.Sales/check_7_1.Units)
    check_7_1 = check_7_1.groupBy('ID', '通用名', '商品名', 'Pack_ID').pivot("Date").agg(func.sum('Price')) \
                        .orderBy('ID', '通用名', '商品名').persist()
    
    # 每家医院的每个产品单价在最近12期的误差范围内（mean+-1.96std或gap10%以内），范围内的产品数量占比大于95%；
    check_7_2 = Raw_data_1.where((Raw_data_1.Date > (current_year-1)*100+current_month-1 ) & (Raw_data_1.Date < current_year*100+current_month)) \
                        .groupBy('ID', 'Date', '通用名', '商品名', 'Pack_ID') \
                        .agg(func.sum('Sales').alias('Sales'), func.sum('Units').alias('Units'))
    check_7_2 = check_7_2.withColumn('Price', check_7_2.Sales/check_7_2.Units)
    check_7_2 = check_7_2.groupBy('ID', '通用名', '商品名', 'Pack_ID') \
                        .agg(func.mean('Price').alias('Mean_Price'), func.stddev('Price').alias('Sd_Price'))
    check_7_2 = check_7_2.withColumn('Sd_Price', func.when(func.isnan(check_7_2.Sd_Price), func.lit(0)).otherwise(check_7_2.Sd_Price))
    Raw_data_1_tmp = Raw_data_1.where(Raw_data_1.Date == current_year*100+current_month) \
                                .groupBy('ID', '通用名', '商品名', 'Pack_ID') \
                                .agg(func.sum('Sales').alias('Sales_newmonth'), func.sum('Units').alias('Units_newmonth'))
    Raw_data_1_tmp = Raw_data_1_tmp.withColumn('Price_newmonth', Raw_data_1_tmp.Sales_newmonth/Raw_data_1_tmp.Units_newmonth)
    Raw_data_1_tmp = Raw_data_1_tmp.withColumn('Pack_ID', func.when(Raw_data_1_tmp.Pack_ID.isNull(), func.lit(0)).otherwise(Raw_data_1_tmp.Pack_ID))
    check_7_2 = check_7_2.withColumn('Pack_ID', func.when(check_7_2.Pack_ID.isNull(), func.lit(0)).otherwise(check_7_2.Pack_ID))
    check_7_2 = check_7_2.join(Raw_data_1_tmp, on=['ID', '通用名', '商品名', 'Pack_ID'], how='left').persist()
    
    check_7_2 = check_7_2.withColumn('Check', \
                func.when((check_7_2.Price_newmonth < check_7_2.Mean_Price-1.96*check_7_2.Sd_Price) & (check_7_2.Price_newmonth < check_7_2.Mean_Price*0.9), func.lit('F')) \
                    .otherwise(func.when((check_7_2.Price_newmonth > check_7_2.Mean_Price+1.96*check_7_2.Sd_Price) & (check_7_2.Price_newmonth > check_7_2.Mean_Price*1.1), func.lit('F')) \
                                    .otherwise(func.lit('T'))))
    check_7_2 = check_7_2.withColumn('Check', func.when(check_7_2.Sales_newmonth.isNull() | check_7_2.Units_newmonth.isNull(), func.lit(None)) \
                                                    .otherwise(check_7_2.Check)) 
    
    check_7_1 = check_7_1.withColumn('Pack_ID', func.when(check_7_1.Pack_ID.isNull(), func.lit(0)).otherwise(check_7_1.Pack_ID))
    check_7 = check_7_1.join(check_7_2, on=['ID', '通用名', '商品名', 'Pack_ID'], how='left').orderBy('ID', '通用名', '商品名').persist()
    check_7 = check_7.withColumn('Pack_ID', func.when(check_7.Pack_ID == 0, func.lit(None)).otherwise(check_7.Pack_ID))
    
    check_7.groupby('Check').count().show()
    
    #========== check_8 ==========
    
    # 每个月产品个数
    check_8 = Raw_data_1.select('Date', 'ID', 'Prod_Name').distinct() \
                        .groupBy('Date').count() \
                        .withColumnRenamed('count', '每月产品个数_min1') \
                        .orderBy('Date').persist()
    # 最近三个月全部医院的产品总数
    check_8_1 = Raw_data_1.where(Raw_data_1.Date.isin(RQMTH)) \
                        .select('Date', 'ID', 'Prod_Name').distinct() \
                        .count()/3
    # 当月月产品个数                  
    check_8_2 = Raw_data_1.where(Raw_data_1.Date.isin(MTH)) \
                        .select('Date', 'ID', 'Prod_Name').distinct() \
                        .count()
    
    check_result_8 = (check_8_2/check_8_1 < 0.03)
    
    check_8 = check_8.repartition(1)
    check_8.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_8_path)
    
    #========== check_9 ==========
    
    # 全部医院的全部产品金额、份额、排名与历史月份对比(含缺失医院)
    check_9_1 = Raw_data_1.groupBy('Date', '商品名').agg(func.sum(Raw_data_1.Sales).alias('Sales')) \
                        .groupBy('商品名').pivot('Date').agg(func.sum('Sales')).persist()
    check_9_2 = Raw_data_1.groupBy('Date', '商品名').agg(func.sum(Raw_data_1.Sales).alias('Sales')) \
                        .join(Raw_data_1.groupBy('Date').agg(func.sum('Sales').alias('Sales_month')), on='Date', how='left')
    check_9_2 = check_9_2.withColumn('share', check_9_2.Sales/check_9_2.Sales_month) \
                        .groupBy('商品名').pivot('Date').agg(func.sum('share')).persist()
    check_9_3 = Raw_data_1.groupBy('Date', '商品名').agg(func.sum(Raw_data_1.Sales).alias('Sales'))
    check_9_3 = check_9_3.withColumn('Rank', func.row_number().over(Window.partitionBy('Date').orderBy(check_9_3['Sales'].desc())))
    check_9_3 = check_9_3.groupBy('商品名').pivot('Date').agg(func.sum('Rank')).persist()
    
    check_9_1 = check_9_1.repartition(1)
    check_9_1.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_9_1_path)
        
    check_9_2 = check_9_2.repartition(1)
    check_9_2.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_9_2_path)
        
    check_9_3 = check_9_3.repartition(1)
    check_9_3.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_9_3_path)
        
    #========== check_10 ==========
    
    # group by 产品和月份，count 医院ID，省份
    # 检查是否有退市产品突然有销量；例如（17120906_2019M12,Pfizer_HTN）
    check_10 = Raw_data_1.select('Date', 'Prod_Name', 'ID').distinct() \
                        .groupBy('Date', 'Prod_Name').count() \
                        .withColumnRenamed('count', '在售产品医院个数') \
                        .groupBy('Prod_Name').pivot('Date').agg(func.sum('在售产品医院个数')).persist()
    
    check_10 = check_10.repartition(1)
    check_10.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_10_path)
        
    #========== check_11 ==========
    
    # 全部医院历史贡献率等级
    allmonth = Raw_data.select('Date').distinct().sort('Date').toPandas()['Date'].values
    
    check_11 = Raw_data.select('ID').distinct().sort('ID')
    index = 0
    for month in allmonth:
        month = int(month)
        tmp = Raw_data.where(Raw_data.Date == month).groupby('ID').agg(func.sum('Sales').alias('Sales')) \
                    .withColumn('tmp', func.lit('tmp'))
        tmp = tmp.withColumn('num',func.row_number().over(Window.partitionBy('tmp').orderBy(tmp['Sales'].desc())))
        tmp = tmp.withColumn('cumsum', func.sum(tmp['Sales']).over(Window.orderBy('num'))) \
                .withColumn('all', func.sum(tmp['Sales']).over(Window.orderBy('tmp')))
        tmp = tmp.withColumn('con_add', tmp.cumsum/tmp.all)
        tmp = tmp.withColumn('Sales_level', func.when(func.ceil(tmp.con_add*10) >10, func.lit(10)).otherwise(func.ceil(tmp.con_add*10))) \
                .select('ID', 'Sales_level') \
                .withColumnRenamed('Sales_level', str(month))
        
        check_11 = check_11.join(tmp, on='ID', how='left')
        
        # 输出避免内存溢出，每20个输出一次
        if (index / 20) % 2 == 0:
            check_11.repartition(1).write.format("parquet") \
                .mode("overwrite").save(tmp_1_path)
            check_11 = spark.read.parquet(tmp_1_path)
        elif (index / 20) % 2 == 1:
            check_11.repartition(1).write.format("parquet") \
                .mode("overwrite").save(tmp_2_path)
            check_11 = spark.read.parquet(tmp_2_path)
        
        index += 1
    
    # 定义的udf返回类型不能包含numpy数据类型，必须强制转化为python基础类型float
    
    #  去掉最大值和最小值后的平均值 
    @udf(DoubleType())
    def mean_adj(*cols):
        import numpy as np
        row_max = cols[0]
        row_min = cols[1]
        others = cols[2:]
        row_mean_list = [x for x in others if x is not None]
        if len(row_mean_list) > 3:
            row_mean = (np.sum(row_mean_list) - row_max - row_min)/(len(row_mean_list) -2)
        else:
            row_mean = 0
        return float(row_mean)
    
    # row_min 与 mean_adj 的差值
    @udf(DoubleType())
    def min_diff(row_max, row_min, mean_adj):
        import numpy as np
        if mean_adj is not None:
            # diff1 = abs(row_max - mean_adj)
            diff2 = abs(row_min - mean_adj)
            row_diff = diff2
        else:
            row_diff = 0
        return float(row_diff)
        
    check_11_cols = check_11.columns
    check_11_cols.remove('ID')
    for each in check_11_cols:
        check_11 = check_11.withColumn(each, check_11[each].cast(IntegerType()))
    
    # 平均值计算
    check_11 = check_11.withColumn("row_max", func.greatest(*check_11_cols)) \
                    .withColumn("row_min", func.least(*check_11_cols))
    check_11 = check_11.withColumn("mean_adj", 
                        mean_adj(func.col('row_max'), func.col('row_min'), *(func.col(x) for x in check_11_cols)))
    check_11 = check_11.withColumn("mean_adj", func.when(func.col('mean_adj') == 0, func.lit(None)).otherwise(func.col('mean_adj')))
    # row_min 与 mean_adj 的差值
    check_11 = check_11.withColumn("min_diff", min_diff(func.col('row_max'), func.col('row_min'), func.col('mean_adj')))
    check_11 = check_11.withColumn("min_diff", func.when(func.col('mean_adj').isNull(), func.lit(None)).otherwise(func.col('min_diff')))
    
    check_11 = check_11.repartition(1)
    check_11.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_11_path)
    
    #========== 汇总检查结果 ==========
    
    # 汇总检查结果
    check_result = spark.createDataFrame(
        [('产品个数与历史相差不超过0.08', str(check_result_1)), 
        ('缺失产品销售额占比不超过0.02', str(check_result_2)), 
        ('医院个数和历史相差不超过0.01', str(check_result_3)), 
        ('缺失医院销售额占比不超过0.01', str(check_result_5)), 
        ('全部医院的全部产品总个数与最近三个月的均值相差不超过0.03', str(check_result_8))], 
        ('check', 'result'))
    
    check_result = check_result.repartition(1)
    check_result.write.format("csv").option("header", "true") \
        .mode("overwrite").save(check_result_path)
    

    



    

    

    




