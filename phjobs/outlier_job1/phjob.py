# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from phlogs.phlogs import phlogger
import os

from pyspark.sql.functions import udf, from_json
import json

def execute(a, b):

    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data from s3") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "1") \
        .config("spark.executor.memory", "1g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .config("spark.sql.crossJoin.enabled","true") \
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
    doi = "AZ16"
    pnl_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/AZ/panel-result_AZ_Sanofi"
    uni_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/AZ/universe_az_sanofi_mch"
    ims_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/AZ/ims_info/"+doi+"_ims_info_1901-1911"
    model_month_left = 201901
    model_month_right = 201911
    prd_input = [u"普米克令舒", u"Others-Pulmicort", u"益索"]
    arg_year = 2019
    
    # 输出
    df_EIA_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/AZ/outlier/"+doi+"/df_EIA"
    df_EIA_res_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/AZ/outlier/"+doi+"/df_EIA_res"
    df_uni_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/AZ/outlier/"+doi+"/df_uni"
    df_seg_city_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/AZ/outlier/"+doi+"/df_seg_city"
    df_hos_city_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/AZ/outlier/"+doi+"/df_hos_city"
    df_ims_shr_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/AZ/outlier/"+doi+"/df_ims_shr"
    
    
    
    # ==============  函数定义 ================
    
    # udf_rename   
    def udf_rename(df_eia, list_prod, suffix=""):
        for p in list_prod:
            df_eia = df_eia.withColumnRenamed("sum("+p+")",
                                              p+suffix)
        return df_eia
    
    # udf_add_struct    
    def udf_add_struct(prd_prod):
        schema = StructType(
            []
        )
        for i in prd_prod:
            schema.add(
                    StructField(i, DoubleType(), True)
                )
        return schema
    
    # max_outlier_poi_job
    def udf_get_poi(pdn):
        #prd_input = [u"加罗宁", u"凯纷", u"诺扬"]
        result = ""
        for item in prd_input:
            if item in pdn:
                result = item
        if result == "":
            return "other"
        else:
            return result
        
    def udf_poi_stack(p, s):
        dic_stack = dict([(i,0) for i in prd_input])
        dic_stack.update({"other": 0, p: s})
        return json.dumps(dic_stack)
        
    # max_outlier_eia_join_uni
    def gen_date_with_year(year):
        result = []
        for month in range(12):
            result.append((1, year * 100 + month + 1))
        return result
    
    # max_outlier_tmp_mod  
    def udf_city_modi(city):
        if city in [u"福州市",u"厦门市",u"泉州市"]:
            return u"福厦泉市"
        elif city in [u"珠海市",u"东莞市",u"中山市",u"佛山市"]:
            return u"珠三角市"
        elif city in [u"金华市",u"台州市",u"嘉兴市",u"绍兴市"]:
            return u"金台嘉绍"
        else:
            return city
        
    max_outlier_poi_udf = udf(udf_get_poi, StringType())
    max_outlier_poi_stack_udf = udf(udf_poi_stack, StringType())
    max_outlier_city_udf = udf(udf_city_modi, StringType())
    
    # ==============  数据执行 ================
    
    # max_outlier_read_df：生成df_EIA, df_uni, df_seg_city, df_hos_city, df_ims_shr
    # max_outlier_poi_job：处理df_EIA，生成df_EIA_res
    # max_outlier_eia_join_uni：处理df_EIA_res
    # max_outlier_tmp_mod：处理df_EIA_res, df_seg_city, df_hos_city
    
    # 1. panel 数据处理，产生df_EIA
    df_EIA = spark.read.parquet(pnl_path)
    df_EIA.persist()

    df_EIA = df_EIA.where((df_EIA.DOI == doi) & (df_EIA.Date >= model_month_left) & (df_EIA.Date <= model_month_right))
    if(doi == "SNY9"):
        df_EIA = df_EIA.where(~df_EIA.Prod_Name.contains("SOLN"))
    if(doi == "AZ7"):
        df_EIA = df_EIA.where(~df_EIA.Prod_Name.contains("和爽") &
                              ~df_EIA.Prod_Name.contains("恒康正清") &
                              ~df_EIA.Prod_Name.contains("福静清") &
                              ~df_EIA.Prod_Name.contains("思然") &
                              ~df_EIA.Prod_Name.contains("北京圣永药业有限公司"))
    if(doi == "AZ12"):
        df_EIA = df_EIA.where(~df_EIA.std_route.contains("OR"))
    if(doi == "AZ14"):
        df_EIA = df_EIA.where(df_EIA.Prod_Name.contains("辅舒酮") |
                              df_EIA.Prod_Name.contains("Others"))
    if(doi == "AZ16"):
        df_EIA = df_EIA.where(~df_EIA.Prod_Name.contains("布地奈德|AERO|64UG|120|台湾健乔信元医药生物股份有限公司") &
                              ~df_EIA.Prod_Name.contains("雷诺考特"))
    if(doi == "AZ19"):
        df_EIA = df_EIA.where(~df_EIA.Prod_Name.contains("Others-Symbicort Cough"))

    df_EIA = df_EIA.withColumn("Year", func.bround(df_EIA.Date / 100))
    
    # 2. uni 数据处理，生成df_uni
    df_uni = spark.read.parquet(uni_path)
    df_seg_city = df_uni.select("City", "Seg").distinct()
    df_hos_city = df_uni.select("Panel_ID", "City").distinct()
    df_uni = df_uni.select("Panel_ID", "Seg", "City", "BEDSIZE", "Est_DrugIncome_RMB", "PANEL")
    df_uni = df_uni.withColumn("key", func.lit(1)).withColumnRenamed("Panel_ID", "HOSP_ID")
    
    df_uni = df_uni.repartition(2)
    df_uni.write.format("parquet") \
        .mode("overwrite").save(df_uni_path)

    # 3. ims 数据处理，生成df_ims_shr
    df_ims_shr = spark.read.parquet(ims_path)\
        .select("city", "poi", "ims_share", "ims_poi_vol")
        
    df_ims_shr = df_ims_shr.repartition(2)
    df_ims_shr.write.format("parquet") \
        .mode("overwrite").save(df_ims_shr_path)
        
    # 4. max_outlier_poi_job：df_EIA 处理，df_EIA_res 生成
    # df_EIA 处理
    df_EIA = df_EIA.withColumn("POI", max_outlier_poi_udf(df_EIA.Prod_Name))
        
    # df_EIA_res 生成
    df_EIA_res = df_EIA.groupBy("ID", "Date", "Hosp_name", "HOSP_ID", "POI", "Year") \
        .agg({
            "Prod_Name": "first",
            "Prod_CNAME": "first",
            "Strength": "first",
            "DOI": "first",
            "DOIE": "first",
            "Sales": "sum",
            "Units": "sum"
        }) \
        .withColumnRenamed("first(Prod_Name)", "Prod_Name") \
        .withColumnRenamed("first(Prod_CNAME)", "Prod_CNAME") \
        .withColumnRenamed("first(Strength)", "Strength") \
        .withColumnRenamed("first(DOI)", "DOI") \
        .withColumnRenamed("first(DOIE)", "DOIE") \
        .withColumnRenamed("sum(Sales)", "Sales") \
        .withColumnRenamed("sum(Units)", "Units")
        
    df_EIA_res.persist()
    df_EIA_res = df_EIA_res.withColumn("value", max_outlier_poi_stack_udf(df_EIA_res.POI, df_EIA_res.Sales))
    schema = udf_add_struct(prd_input+["other"])       
    
    df_EIA_res = df_EIA_res.select(
        "ID", "Date", "Hosp_name", "HOSP_ID", "Year",
        from_json(df_EIA_res.value, schema).alias("json")
    ).select(
        "ID", "Date", "Hosp_name", "HOSP_ID", "Year",
        "json.*")

    df_EIA_res = df_EIA_res.groupBy("ID", "Date", "Hosp_name", "HOSP_ID", "Year").\
        sum(*prd_input+["other"])
    print df_EIA_res.columns
    df_EIA_res = udf_rename(df_EIA_res, prd_input + ["other"])
        
    # 4. max_outlier_eia_join_uni：处理universe join EIA
    # arg_year = 2019
    date = gen_date_with_year(arg_year)
    schema = StructType([StructField("key", IntegerType(), True), StructField("Date", IntegerType(), True)])
    date = spark.createDataFrame(date, schema)
    df_uni = df_uni.join(date, on="key", how="outer")
    df_EIA_res = df_uni.join(df_EIA_res, on=["HOSP_ID", "Date"], how="left")
    
    # 5. max_outlier_tmp_mod：对福建，厦门，泉州，珠江三角的调整需要
    df_EIA_res = df_EIA_res.withColumn("City", max_outlier_city_udf(df_EIA_res.City))
    df_seg_city = df_seg_city.withColumn("City", max_outlier_city_udf(df_seg_city.City))
    df_hos_city = df_hos_city.withColumn("City", max_outlier_city_udf(df_hos_city.City))
    
    df_EIA = df_EIA.repartition(2)
    df_EIA.write.format("parquet") \
        .mode("overwrite").save(df_EIA_path)
        
    df_EIA_res = df_EIA_res.repartition(2)
    df_EIA_res.write.format("parquet") \
        .mode("overwrite").save(df_EIA_res_path)
        
    df_seg_city = df_seg_city.repartition(2)
    df_seg_city.write.format("parquet") \
        .mode("overwrite").save(df_seg_city_path)
        
    df_hos_city = df_hos_city.repartition(2)
    df_hos_city.write.format("parquet") \
        .mode("overwrite").save(df_hos_city_path)
    
    return [df_EIA, df_EIA_res, df_uni, df_seg_city, df_hos_city, df_ims_shr]
        
    