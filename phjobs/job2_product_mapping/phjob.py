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
    minimum_product_columns = kwargs['minimum_product_columns']
    minimum_product_sep = kwargs['minimum_product_sep']
    minimum_product_newname = kwargs['minimum_product_newname']
    need_cleaning_cols = kwargs['need_cleaning_cols']
    if_others = kwargs['if_others']
    out_path = kwargs['out_path']
    out_dir = kwargs['out_dir']
    need_test = kwargs['need_test']
    ### input args ###
    
    ### output args ###
    a = kwargs['a']
    b = kwargs['b']
    ### output args ###

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    # %%
    # project_name = '京新'
    # out_dir = 'test'
    # %%
    logger.debug('job2_product_mapping')
    
    # 注意：
    # Mylan不做Brand判断，写死了
    # Mylan不重新生成minimum_product_newname: min1
    # 输入
    product_map_path = out_path + "/" + project_name + '/' + out_dir + "/prod_mapping"
    
    if if_others == "True":
        out_dir = out_dir + "/others_box/"
    out_path_dir = out_path + "/" + project_name + '/' + out_dir
    
    hospital_mapping_out_path = out_path_dir + "/hospital_mapping_out"
    need_cleaning_cols = need_cleaning_cols.replace(" ","").split(",")
    minimum_product_columns = minimum_product_columns.replace(" ","").split(",")
    if minimum_product_sep == "kong":
        minimum_product_sep = ""
    # 输出
    product_mapping_out_path = out_path_dir + "/product_mapping_out"
    need_cleaning_path = out_path_dir + "/need_cleaning"

    # %%
    # =========== 数据检查 =============
    logger.debug('数据检查-start')
    # 存储文件的缺失列
    misscols_dict = {}
    # product_map file
    # product_map_path = "/common/projects/max/Sankyo/prod_mapping"
    product_map = spark.read.parquet(product_map_path)
    colnames_product_map = product_map.columns
    misscols_dict.setdefault("product_map", [])
    if ("标准通用名" not in colnames_product_map) and ("通用名_标准"  not in colnames_product_map)  \
    and ("药品名称_标准"  not in colnames_product_map) and ("通用名"  not in colnames_product_map) \
    and ("S_Molecule_Name"  not in colnames_product_map):
        misscols_dict["product_map"].append("标准通用名")
    if "min1" not in colnames_product_map:
        misscols_dict["product_map"].append("min1")
    if ("min2" not in colnames_product_map) and ("min1_标准" not in colnames_product_map):
        misscols_dict["product_map"].append("min2")
    if ("标准商品名" not in colnames_product_map) and ("商品名_标准"  not in colnames_product_map) \
    and ("S_Product_Name"  not in colnames_product_map):
        misscols_dict["product_map"].append("标准商品名")
    # 判断输入文件是否有缺失列
    misscols_dict_final = {}
    for eachfile in misscols_dict.keys():
        if len(misscols_dict[eachfile]) != 0:
            misscols_dict_final[eachfile] = misscols_dict[eachfile]
    # 如果有缺失列，则报错，停止运行
    if misscols_dict_final:
        logger.debug('miss columns: %s' % (misscols_dict_final))
        raise ValueError('miss columns: %s' % (misscols_dict_final))
    logger.debug('数据检查-Pass')

    # %%
    # =========== 数据执行 =============
    logger.debug('数据执行-start')
    # raw_data_job1_out_path = "/user/ywyuan/max/Sankyo/raw_data_job1_out"
    raw_data = spark.read.parquet(hospital_mapping_out_path)
    # if project_name != "Mylan":
    raw_data = raw_data.withColumn("Brand", func.when((raw_data.Brand.isNull()) | (raw_data.Brand == 'NA'), raw_data.Molecule).
                                   otherwise(raw_data.Brand))
    # concat_multi_cols
    # minimum_product_columns = ["Brand", "Form", "Specifications", "Pack_Number", "Manufacturer"]
    # minimum_product_sep = ""
    # minimum_product_newname = "min1"
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
    #    raw_data = raw_data.drop("tmp")
    # else:
    if minimum_product_newname in raw_data.columns:
        raw_data = raw_data.drop(minimum_product_newname)
    raw_data = raw_data.withColumnRenamed("tmp", minimum_product_newname)
    # product_map
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
    product_map_for_needclean = product_map.select("min1").distinct()
    product_map_for_rawdata = product_map.select("min1", "min2", "通用名", "std_route", "标准商品名").distinct()
    # 输出待清洗
    need_cleaning = raw_data.join(product_map_for_needclean, on="min1", how="left_anti") \
        .select(need_cleaning_cols) \
        .distinct()
    logger.debug('待清洗行数: ' + str(need_cleaning.count()))
    if need_cleaning.count() > 0:
        need_cleaning = need_cleaning.repartition(2)
        need_cleaning.write.format("parquet") \
            .mode("overwrite").save(need_cleaning_path)
        logger.debug("已输出待清洗文件至:  " + need_cleaning_path)
    raw_data = raw_data.join(product_map_for_rawdata, on="min1", how="left") \
        .drop("S_Molecule") \
        .withColumnRenamed("通用名", "S_Molecule")
    # %%
    product_mapping_out = raw_data.repartition(2)
    product_mapping_out.write.format("parquet") \
        .mode("overwrite").save(product_mapping_out_path)
    logger.debug("输出 product_mapping 结果：" + product_mapping_out_path)
    logger.debug('数据执行-Finish')
