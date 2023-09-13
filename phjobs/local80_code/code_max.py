# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

def job1_hospital_mapping(kwargs):
    ### input args ###
    max_path = kwargs.get('max_path', 'D:/Auto_MAX/MAX/')
    project_name = kwargs['project_name']
    cpa_gyc = kwargs.get('cpa_gyc', 'True')
    if_others = kwargs.get('if_others','False')
    out_path = kwargs.get('out_path', 'D:/Auto_MAX/MAX/')
    out_dir = kwargs['out_dir']
    auto_max = kwargs.get('auto_max','True')
    need_test = kwargs.get('need_test','0')
    ### input args ###

    ### output args ###
    ### output args ###

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col

    spark = SparkSession.builder.getOrCreate()
    
    if auto_max == "False":
        raise ValueError('auto_max: False 非自动化')

    print('job1_hospital_mapping')
    # 输入
    if if_others != "False" and if_others != "True":
        logger.error('wrong input: if_others, False or True')
        raise ValueError('wrong input: if_others, False or True')

    universe_path = max_path + "/" + project_name + "/universe_base"
    cpa_pha_mapping_path = max_path + "/" + project_name + "/cpa_pha_mapping"
    if if_others == "True":
        raw_data_path = max_path + "/" + project_name + "/" + out_dir + "/raw_data_box"
    else:
        raw_data_path = max_path + "/" + project_name + "/" + out_dir + "/raw_data"

    # 输出
    if if_others == "True":
        out_dir = out_dir + "/others_box/"
    hospital_mapping_out_path = out_path + "/" + project_name + "/" + out_dir + "/hospital_mapping_out"
    # =========== 数据检查 =============
    print('数据检查-start')
    # 存储文件的缺失列
    misscols_dict = {}
    # universe file
    # universe_path = "/common/projects/max/Sankyo/universe_base"
    universe = spark.read.parquet(universe_path)
    colnames_universe = universe.columns
    misscols_dict.setdefault("universe", [])
    if ("City_Tier" not in colnames_universe) and ("CITYGROUP" not in colnames_universe) and (
            "City_Tier_2010" not in colnames_universe):
        misscols_dict["universe"].append("City_Tier/CITYGROUP")
    if ("Panel_ID" not in colnames_universe) and ("PHA" not in colnames_universe):
        misscols_dict["universe"].append("Panel_ID/PHA")
    # if ("Hosp_name" not in colnames_universe) and ("HOSP_NAME" not in colnames_universe):
    #    misscols_dict["universe"].append("Hosp_name/HOSP_NAME")
    if "City" not in colnames_universe:
        misscols_dict["universe"].append("City")
    if "Province" not in colnames_universe:
        misscols_dict["universe"].append("Province")
    # cpa_pha_mapping file
    # cpa_pha_mapping_path = "/common/projects/max/Sankyo/cpa_pha_mapping"
    cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
    colnames_map = cpa_pha_mapping.columns
    misscols_dict.setdefault("cpa_pha_mapping", [])
    if "推荐版本" not in colnames_map:
        misscols_dict["cpa_pha_mapping"].append("推荐版本")
    if ("ID" not in colnames_map) and ("BI_hospital_code" not in colnames_map):
        misscols_dict["cpa_pha_mapping"].append("ID/BI_hospital_code")
    if ("PHA" not in colnames_map) and ("PHA_ID_x" not in colnames_map):
        misscols_dict["cpa_pha_mapping"].append("PHA")
    # raw_data file
    # raw_data_path = "/common/projects/max/Sankyo/raw_data"
    raw_data = spark.read.parquet(raw_data_path)
    colnames_raw_data = raw_data.columns
    misscols_dict.setdefault("raw_data", [])
    if ("Units" not in colnames_raw_data) and ("数量（支/片）" not in colnames_raw_data) and (
            "最小制剂单位数量" not in colnames_raw_data) and ("total_units" not in colnames_raw_data) and (
            "SALES_QTY" not in colnames_raw_data):
        misscols_dict["raw_data"].append("about Units")
    if ("Sales" not in colnames_raw_data) and ("金额（元）" not in colnames_raw_data) and (
            "金额" not in colnames_raw_data) and ("sales_value__rmb_" not in colnames_raw_data) and (
            "SALES_VALUE" not in colnames_raw_data):
        misscols_dict["raw_data"].append("about Sales")
    if ("year_month" not in colnames_raw_data) and ("Yearmonth" not in colnames_raw_data) and (
            "YM" not in colnames_raw_data) and ("Date" not in colnames_raw_data):
        misscols_dict["raw_data"].append("about year_month")
    if "ID" not in colnames_raw_data:
        misscols_dict["raw_data"].append("ID")
    if ("Molecule" not in colnames_raw_data) and ("通用名" not in colnames_raw_data) and (
            "药品名称" not in colnames_raw_data) and ("molecule_name" not in colnames_raw_data) and (
            "MOLE_NAME" not in colnames_raw_data):
        misscols_dict["raw_data"].append("about Molecule")
    if ("Brand" not in colnames_raw_data) and ("商品名" not in colnames_raw_data) and (
            "药品商品名" not in colnames_raw_data) and ("product_name" not in colnames_raw_data) and (
            "PRODUCT_NAME" not in colnames_raw_data):
        misscols_dict["raw_data"].append("about Brand")
    if ("Specifications" not in colnames_raw_data) and ("规格" not in colnames_raw_data) and (
            "pack_description" not in colnames_raw_data) and ("SPEC" not in colnames_raw_data):
        misscols_dict["raw_data"].append("about Specifications")
    if ("Form" not in colnames_raw_data) and ("剂型" not in colnames_raw_data) and (
            "formulation_name" not in colnames_raw_data) and ("DOSAGE" not in colnames_raw_data):
        misscols_dict["raw_data"].append("about Form")
    if ("Manufacturer" not in colnames_raw_data) and ("生产企业" not in colnames_raw_data) and (
            "company_name" not in colnames_raw_data) and ("MANUFACTURER_NAME" not in colnames_raw_data):
        misscols_dict["raw_data"].append("about Manufacturer")
    # 判断输入文件是否有缺失列
    misscols_dict_final = {}
    for eachfile in misscols_dict.keys():
        if len(misscols_dict[eachfile]) != 0:
            misscols_dict_final[eachfile] = misscols_dict[eachfile]
    # 如果有缺失列，则报错，停止运行
    if misscols_dict_final:
        print('miss columns: %s' % (misscols_dict_final))
        raise ValueError('miss columns: %s' % (misscols_dict_final))
    print('数据检查-Pass')

    # %%
    # =========== 数据执行 =============
    print('数据执行-start')
    # 1. 首次补数
    # read_universe
    if "CITYGROUP" in universe.columns:
        universe = universe.withColumnRenamed("CITYGROUP", "City_Tier_2010")
    elif "City_Tier" in universe.columns:
        universe = universe.withColumnRenamed("City_Tier", "City_Tier_2010")
    universe = universe.withColumnRenamed("Panel_ID", "PHA") \
        .withColumnRenamed("Hosp_name", "HOSP_NAME")

    universe = universe.withColumn("City_Tier_2010", universe["City_Tier_2010"].cast(StringType()))
    PHA_city_in_universe = universe.select("PHA", "City", "City_Tier_2010").distinct()

    # 1.2 读取CPA与PHA的匹配关系:
    # map_cpa_pha
    def deal_ID_length(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字，其他还有包含字母的ID
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df

    cpa_pha_mapping = deal_ID_length(cpa_pha_mapping)
    cpa_pha_mapping = cpa_pha_mapping.filter(cpa_pha_mapping["推荐版本"] == 1) \
        .withColumnRenamed("ID", "BI_hospital_code") \
        .withColumnRenamed("PHA", "PHA_ID_x") \
        .select("PHA_ID_x", "BI_hospital_code").distinct()
    cpa_pha_mapping = cpa_pha_mapping.join(universe.select("PHA", "Province", "City"),
                                           cpa_pha_mapping.PHA_ID_x == universe.PHA,
                                           how="left") \
        .drop("PHA")
    # %%
    # 1.3 读取原始样本数据:
    # read_raw_data
    raw_data = deal_ID_length(raw_data)
    raw_data = raw_data.withColumnRenamed("ID", "BI_Code") \
        .drop("Province", "City")
    raw_data = raw_data.join(cpa_pha_mapping.select("PHA_ID_x", "BI_hospital_code", 'Province', 'City'),
                             raw_data.BI_Code == cpa_pha_mapping.BI_hospital_code,
                             how="left")
    # format_raw_data
    # cpa_gyc = True
    for col in raw_data.columns:
        if col in ["数量（支/片）", "最小制剂单位数量", "total_units", "SALES_QTY"]:
            raw_data = raw_data.withColumnRenamed(col, "Units")
        if col in ["金额（元）", "金额", "sales_value__rmb_", "SALES_VALUE"]:
            raw_data = raw_data.withColumnRenamed(col, "Sales")
        if col in ["Yearmonth", "YM", "Date"]:
            raw_data = raw_data.withColumnRenamed(col, "year_month")
        if col in ["年", "年份", "YEAR"]:
            raw_data = raw_data.withColumnRenamed(col, "Year")
        if col in ["月", "月份", "MONTH"]:
            raw_data = raw_data.withColumnRenamed(col, "Month")
        if col in ["医院编码", "BI_Code", "HOSP_CODE"]:
            raw_data = raw_data.withColumnRenamed(col, "ID")
        if col in ["通用名", "药品名称", "molecule_name", "MOLE_NAME"]:
            raw_data = raw_data.withColumnRenamed(col, "Molecule")
        if col in ["商品名", "药品商品名", "product_name", "PRODUCT_NAME"]:
            raw_data = raw_data.withColumnRenamed(col, "Brand")
        if col in ["规格", "pack_description", "SPEC"]:
            raw_data = raw_data.withColumnRenamed(col, "Specifications")
        if col in ["剂型", "formulation_name", "DOSAGE"]:
            raw_data = raw_data.withColumnRenamed(col, "Form")
        if col in ["包装数量", "包装规格", "PACK_QTY"]:
            raw_data = raw_data.withColumnRenamed(col, "Pack_Number")
        if col in ["生产企业", "company_name", "MANUFACTURER_NAME"]:
            raw_data = raw_data.withColumnRenamed(col, "Manufacturer")
        if col in ["省份", "省", "省/自治区/直辖市", "province_name", "PROVINCE_NAME"]:
            raw_data = raw_data.withColumnRenamed(col, "Province")
        if col in ["城市", "city_name", "CITY_NAME"]:
            raw_data = raw_data.withColumnRenamed(col, "City")
        if col in ["PHA_ID_x", "PHA_ID"]:
            raw_data = raw_data.withColumnRenamed(col, "PHA")

    # %%
    # ID 的长度统一
    def distinguish_cpa_gyc(col, gyc_hospital_id_length):
        # gyc_hospital_id_length是国药诚信医院编码长度，一般是7位数字，cpa医院编码一般是6位数字。医院编码长度可以用来区分cpa和gyc
        return (func.length(col) < gyc_hospital_id_length)

    def deal_ID_length(df):
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(distinguish_cpa_gyc(df.ID, 7), func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df

    raw_data = deal_ID_length(raw_data)
    if "year_month" in raw_data.columns:
        raw_data = raw_data.withColumn("year_month", raw_data["year_month"].cast(IntegerType()))
    if "Month" not in raw_data.columns:
        raw_data = raw_data.withColumn("Month", raw_data.year_month % 100)
    if "Pack_Number" not in raw_data.columns:
        raw_data = raw_data.withColumn("Pack_Number", func.lit(0))
    if "Year" not in raw_data.columns:
        raw_data = raw_data.withColumn("Year", (raw_data.year_month - raw_data.Month) / 100)
    raw_data = raw_data.withColumn("Year", raw_data["Year"].cast(IntegerType())) \
        .withColumn("Month", raw_data["Month"].cast(IntegerType()))
    raw_data = raw_data.join(PHA_city_in_universe, on=["PHA", "City"], how="left")
    # %%
    hospital_mapping_out = raw_data.repartition(2)
    hospital_mapping_out.write.format("parquet") \
        .mode("overwrite").save(hospital_mapping_out_path)
    print("输出 hospital_mapping 结果：" + hospital_mapping_out_path)
    print('数据执行-Finish')

def job2_product_mapping(kwargs):
    ### input args ###
    max_path = kwargs.get('max_path', 'D:/Auto_MAX/MAX/')
    project_name = kwargs['project_name']
    minimum_product_columns = kwargs.get('minimum_product_columns', 'Brand, Form, Specifications, Pack_Number, Manufacturer')
    minimum_product_sep = kwargs.get('minimum_product_sep', '|')
    minimum_product_newname = kwargs.get('minimum_product_newname', 'min1')
    need_cleaning_cols = kwargs.get('need_cleaning_cols', 'Molecule, Brand, Form, Specifications, Pack_Number, Manufacturer, min1, Route, Corp')
    if_others = kwargs.get('if_others', 'False')
    out_path = kwargs.get('out_path', 'D:/Auto_MAX/MAX/')
    out_dir = kwargs['out_dir']
    need_test = kwargs.get('need_test', '0')
    ### input args ###
    

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    # %%
    # project_name = '京新'
    # out_dir = 'test'
    # %%
    
    spark = SparkSession.builder.getOrCreate()
    
    print('job2_product_mapping')
    
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
    print('数据检查-start')
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
        print('miss columns: %s' % (misscols_dict_final))
        raise ValueError('miss columns: %s' % (misscols_dict_final))
    print('数据检查-Pass')

    # %%
    # =========== 数据执行 =============
    print('数据执行-start')
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
    product_map_for_rawdata = product_map.select("min1", "min2", "通用名", "标准商品名").distinct()
    # 输出待清洗
    need_cleaning = raw_data.join(product_map_for_needclean, on="min1", how="left_anti") \
        .select(need_cleaning_cols) \
        .distinct()
    print('待清洗行数: ' + str(need_cleaning.count()))
    if need_cleaning.count() > 0:
        need_cleaning = need_cleaning.repartition(2)
        need_cleaning.write.format("parquet") \
            .mode("overwrite").save(need_cleaning_path)
        print("已输出待清洗文件至:  " + need_cleaning_path)
    raw_data = raw_data.join(product_map_for_rawdata, on="min1", how="left") \
        .drop("S_Molecule") \
        .withColumnRenamed("通用名", "S_Molecule")
    # %%
    product_mapping_out = raw_data.repartition(2)
    product_mapping_out.write.format("parquet") \
        .mode("overwrite").save(product_mapping_out_path)
    print("输出 product_mapping 结果：" + product_mapping_out_path)
    print('数据执行-Finish')


def job3_1_data_adding(kwargs):
    ### input args ###
    max_path = kwargs.get('max_path', 'D:/Auto_MAX/MAX/')
    project_name = kwargs['project_name']
    model_month_right = kwargs['model_month_right']
    max_month = kwargs.get('max_month', '0')
    year_missing = kwargs.get('year_missing', '0')
    current_year = kwargs.get('current_year', 'Empty')
    first_month = kwargs.get('first_month', 'Empty')
    current_month = kwargs.get('current_month', 'Empty')
    if_others = kwargs.get('if_others', 'False')
    monthly_update = kwargs['monthly_update']
    not_arrived_path = kwargs.get('not_arrived_path', 'Empty')
    published_path = kwargs.get('published_path', 'Empty')
    out_path = kwargs.get('out_path', 'D:/Auto_MAX/MAX/')
    out_dir = kwargs['out_dir']
    need_test = kwargs.get('need_test', '0')
    if_add_data = kwargs.get('if_add_data', 'True')
    ### input args ###
    

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    import pandas as pd
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    # %%
    # project_name = '京新'
    # out_dir = 'test'
    # monthly_update = "True"
    # model_month_left = "201901"
    # model_month_right = "201912"
    # all_models = "康复新液,益生菌,癫痫,他汀,帕金森,癫痫新分子"
    # universe_choice = "他汀:universe_他汀"
    # need_cleaning_cols = "Molecule, Brand, Form, Specifications, Pack_Number, Manufacturer, min1"
    # time_left = "202001"
    # time_right = "202004"
    # first_month = "1"
    # current_month = "4"
    # %%
    '''
    注意：杨森，月更新补数脚本特殊处理已经写脚本中
    '''
    spark = SparkSession.builder.getOrCreate()
    
    print('job3_1_data_adding')
    
    if if_add_data != "False" and if_add_data != "True":
        print('wrong input: if_add_data, False or True') 
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
        print('wrong input: monthly_update, False or True') 
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
    price_city_path = out_path_dir + "/price_city"
    growth_rate_path = out_path_dir + "/growth_rate"
    adding_data_path =  out_path_dir + "/adding_data"
    raw_data_adding_path =  out_path_dir + "/raw_data_adding"
    new_hospital_path = out_path_dir + "/new_hospital"
    raw_data_adding_final_path =  out_path_dir + "/raw_data_adding_final"
    # %%
    # =========== 数据检查 =============
    print('数据检查-start')
    
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
            print('ID length error: %s' % (length_error_dict))
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
    'Month', 'Year', 'City_Tier_2010', 'min2', 'S_Molecule', "标准商品名"]
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
        print('miss columns: %s' % (misscols_dict_final))
        raise ValueError('miss columns: %s' % (misscols_dict_final))
    print('数据检查-Pass')
    # %%
    # =========== 数据执行 =============
    print('数据执行-start')
    # 数据读取
    raw_data = spark.read.parquet(product_mapping_out_path)
    raw_data = raw_data.withColumn('City_Tier_2010', col('City_Tier_2010').cast('int'))
    raw_data.persist()
    products_of_interest = spark.read.csv(products_of_interest_path, header=True)
    products_of_interest = products_of_interest.toPandas()["poi"].values.tolist()
    # raw_data 处理
    raw_data = raw_data.withColumn("S_Molecule_for_gr",
                                   func.when(raw_data["标准商品名"].isin(products_of_interest), raw_data["标准商品名"]).
                                   otherwise(raw_data.S_Molecule))
    print('1 价格计算')
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
    
    # 城市层面
    price_city = raw_data.groupBy("min2", "year_month", 'City', 'Province') \
                        .agg((func.sum("Sales") / func.sum("Units")).alias("Price"))
    price_city = price_city.where(~price_city.Price.isNull())
    # 输出price
    price = price.repartition(2)
    price.write.format("parquet") \
        .mode("overwrite").save(price_path)
    
    price_city = price_city.repartition(2)
    price_city.write.format("parquet") \
        .mode("overwrite").save(price_city_path)
    print("输出 price：" + price_path)
    # raw_data 处理
    if monthly_update == "False":
        raw_data = raw_data.where(raw_data.Year < ((model_month_right // 100) + 1))
        if project_name == "Sanofi" or project_name == "AZ":
            raw_data = raw_data.where(raw_data.Year > 2016)
    elif monthly_update == "True":
        if project_name == "Sanofi" or project_name == "AZ":
            raw_data = raw_data.where(raw_data.Year > 2016)
    '''
    print('2 连续性计算')
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
    '''
    # 3 计算样本分子增长率: cal_growth
    def calculate_growth(raw_data, max_month=12):
        # TODO: 完整年用完整年增长，不完整年用不完整年增长
        if max_month < 12:
            raw_data = raw_data.where(raw_data.Month <= max_month)
        # raw_data 处理
        growth_raw_data = raw_data.na.fill({"City_Tier_2010": 5})
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
    print('3 增长率计算')
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
        published_left = published_left.select('ID').distinct()
        
        published_right = spark.read.csv(published_right_path, header=True)
        published_right = published_right.select('ID').distinct()
        
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
                    
            print("输出 growth_rate：" + growth_rate_path)
                
    
    if monthly_update == "False":
        growth_rate = growth_rate.repartition(2)
        growth_rate.write.format("parquet") \
            .mode("overwrite").save(growth_rate_path)
        print("输出 growth_rate：" + growth_rate_path)
    elif monthly_update == "True":
        growth_rate = spark.read.parquet(growth_rate_path)
    print('数据执行-Finish')
 

def job3_2_data_adding(kwargs):
    ### input args ###
    max_path = kwargs.get('max_path', 'D:/Auto_MAX/MAX/')
    project_name = kwargs['project_name']
    model_month_right = kwargs['model_month_right']
    max_month = kwargs.get('max_month', '0')
    year_missing = kwargs.get('year_missing', '0')
    current_year = kwargs.get('current_year', 'Empty')
    first_month = kwargs.get('first_month', 'Empty')
    current_month = kwargs.get('current_month', 'Empty')
    if_others = kwargs.get('if_others', 'False')
    monthly_update = kwargs['monthly_update']
    not_arrived_path = kwargs.get('not_arrived_path', 'Empty')
    published_path = kwargs.get('published_path', 'Empty')
    out_path = kwargs.get('out_path', 'D:/Auto_MAX/MAX/')
    out_dir = kwargs['out_dir']
    need_test = kwargs.get('need_test', '0')
    if_add_data = kwargs.get('if_add_data', 'True')
    ### input args ###

    
    import pandas as pd
    import os
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StringType, IntegerType, DoubleType
    from pyspark.sql import functions as func
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col  
    from pyspark.sql import Window
    # %%
    # 测试用
    # project_name = "Gilead"
    # model_month_right = "201912"
    # first_month = "1"
    # current_month = "1"
    # monthly_update = "True"
    # out_dir = "202101"
    # current_year = '2021'
    # %%
    
    spark = SparkSession.builder.getOrCreate()
    
    print('job3_2_data_adding')
    
    if if_add_data != "False" and if_add_data != "True":
        logger.error('wrong input: if_add_data, False or True') 
        raise ValueError('wrong input: if_add_data, False or True')
    
    if if_others == "True":
        out_dir = out_dir + "/others_box/"
    
    out_path_dir = out_path + "/" + project_name + '/' + out_dir
    
    # 输入
    product_mapping_out_path = out_path_dir + "/product_mapping_out"
    products_of_interest_path = max_path + "/" + project_name + "/poi.csv"
    cpa_pha_mapping_path = max_path + "/" + project_name + "/cpa_pha_mapping"
    
    if year_missing:
        year_missing = year_missing.replace(" ","").split(",")
    else:
        year_missing = []
    year_missing = [int(i) for i in year_missing]
    model_month_right = int(model_month_right)
    max_month = int(max_month)
    
    # 月更新相关参数
    if monthly_update != "False" and monthly_update != "True":
        print('wrong input: monthly_update, False or True') 
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
    else:
        current_year = model_month_right//100
    
    # published_left_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/Published2019.csv"
    # published_right_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Common_files/Published2020.csv"
    
    # 输出
    price_path = out_path_dir + "/price"
    price_city_path = out_path_dir + "/price_city"
    growth_rate_path = out_path_dir + "/growth_rate"
    adding_data_path =  out_path_dir + "/adding_data"
    adding_data_tmp_path =  out_path_dir + "/adding_data_tmp"
    raw_data_adding_path =  out_path_dir + "/raw_data_adding"
    new_hospital_path = out_path_dir + "/new_hospital"
    raw_data_adding_final_path =  out_path_dir + "/raw_data_adding_final"

    # %%
    # =========== 数据检查 =============
    
    print('数据检查-start')
    
    # 存储文件的缺失列
    misscols_dict = {}
    
    # product_mapping_out file
    raw_data = spark.read.parquet(product_mapping_out_path)
    colnames_raw_data = raw_data.columns
    misscols_dict.setdefault("product_mapping_out", [])
    
    colnamelist = ['min1', 'PHA', 'City', 'year_month', 'ID',  'Brand', 'Form',
    'Specifications', 'Pack_Number', 'Manufacturer', 'Molecule', 'Source',
    'Sales', 'Units', 'Path', 'Sheet', 'BI_hospital_code', 'Province', 
    'Month', 'Year', 'City_Tier_2010', 'min2', 'S_Molecule', "标准商品名"]
    #'Raw_Hosp_Name','ORG_Measure','Units_Box', 'Corp', 'Route',
    
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
        print('miss columns: %s' % (misscols_dict_final))
        raise ValueError('miss columns: %s' % (misscols_dict_final))
    
    print('数据检查-Pass')

    # %%
    # =========== 数据准备 =============
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
    
    def deal_ID_length(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字，其他还有包含字母的ID
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df
    
    # 一. 生成 original_range_raw（样本中已到的Year、Month、PHA的搭配）
    # 2017到当前年的全量出版医院
    Published_years = list(range(2017, current_year+1, 1))
    for index, eachyear in enumerate(Published_years):
        allmonth = [str(eachyear*100 + i) for i in list(range(1,13,1))]
        published_path = max_path + "/Common_files/Published"+str(eachyear)+".csv"
        published = spark.read.csv(published_path, header=True)
        published = published.where(col('Source') == 'CPA').select('ID').distinct()
        published = deal_ID_length(published)
        for i in allmonth:
            published = published.withColumn(i, func.lit(1))
        if index == 0:
            published_full = published
        else:
            published_full = published_full.join(published, on='ID', how='full')
    
    published_all = unpivot(published_full, ['ID'])
    published_all = published_all.where(col('value')==1).withColumnRenamed('feature', 'Date') \
                                .drop('value')
    
    # 模型前之前的未到名单（跑模型年的时候，不去除未到名单） 
    if monthly_update == 'True':        
        # 1.当前年的未到名单
        not_arrived_current = spark.read.csv(not_arrived_path, header=True)
        not_arrived_current = not_arrived_current.select('ID', 'Date').distinct()
        not_arrived_current = deal_ID_length(not_arrived_current)
        # 2.其他模型年之后的未到名单
        model_year = model_month_right//100
        not_arrived_others_years = set((range(model_year+1, current_year+1, 1)))-set([current_year])
        if not_arrived_others_years:
            for index, eachyear in enumerate(not_arrived_others_years):
                not_arrived_others_path = max_path + "/Common_files/Not_arrived"+str(eachyear)+"12.csv"
                #print(not_arrived_others_path)
                not_arrived = spark.read.csv(not_arrived_others_path, header=True)
                not_arrived = not_arrived.select('ID', 'Date').distinct()
                not_arrived = deal_ID_length(not_arrived)
                if index == 0:
                    not_arrived_others = not_arrived
                else:
                    not_arrived_others = not_arrived_others.union(not_arrived)
            not_arrived_all = not_arrived_current.union(not_arrived_others)
        else:
            not_arrived_all = not_arrived_current
    
            
    original_range_raw_noncpa = raw_data.where(col('Source') != 'CPA').select('ID', 'year_month').distinct() \
                                        .withColumnRenamed('year_month', 'Date')
    # 出版医院 减去 未到名单
    if monthly_update == 'True':
        original_range_raw = published_all.join(not_arrived_all, on=['ID', 'Date'], how='left_anti')
    else:
        original_range_raw = published_all
        
    original_range_raw = original_range_raw.union(original_range_raw_noncpa.select(original_range_raw.columns))
        
    # 匹配 PHA
    cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
    cpa_pha_mapping = cpa_pha_mapping.filter(cpa_pha_mapping["推荐版本"] == 1) \
                                    .select("ID", "PHA").distinct()
    cpa_pha_mapping = deal_ID_length(cpa_pha_mapping)
    
    original_range_raw = original_range_raw.join(cpa_pha_mapping, on='ID', how='left')
    original_range_raw = original_range_raw.where(~col('PHA').isNull()) \
                                            .withColumn('Year', func.substring(col('Date'), 0, 4)) \
                                            .withColumn('Month', func.substring(col('Date'), 5, 2).cast(IntegerType())) \
                                            .select('PHA', 'Year', 'Month').distinct()

    # %%
    # =========== 数据执行 =============
    print('数据执行-start')
    
    # 数据读取
    raw_data = spark.read.parquet(product_mapping_out_path)
    raw_data.persist()
    products_of_interest = spark.read.csv(products_of_interest_path, header=True)
    products_of_interest = products_of_interest.toPandas()["poi"].values.tolist()
    
    # raw_data 处理
    raw_data = raw_data.withColumn("S_Molecule_for_gr",
                                   func.when(raw_data["标准商品名"].isin(products_of_interest), raw_data["标准商品名"]).
                                   otherwise(raw_data.S_Molecule))
    
    price = spark.read.parquet(price_path)
    price = price.withColumnRenamed('Price', 'Price_tier')
    
    growth_rate = spark.read.parquet(growth_rate_path)
    growth_rate = growth_rate.fillna(1, subset= [i for i in growth_rate.columns if i.startswith("GR")])
    growth_rate.persist()
    
    price_city = spark.read.parquet(price_city_path)
    price_city = price_city.withColumnRenamed('Price', 'Price_city')
    
    
    # raw_data 处理
    if monthly_update == "False":
        raw_data = raw_data.where(raw_data.Year < ((model_month_right // 100) + 1))
        if project_name == "Sanofi" or project_name == "AZ":
            raw_data = raw_data.where(raw_data.Year > 2016)
    elif monthly_update == "True":
        if project_name == "Sanofi" or project_name == "AZ":
            raw_data = raw_data.where(raw_data.Year > 2016)
    
    # 4 补数
    def add_data(raw_data, growth_rate):
        # 4.1 原始数据格式整理， 用于补数: trans_raw_data_for_adding
        growth_rate = growth_rate.select(["CITYGROUP", "S_Molecule_for_gr"] + [name for name in growth_rate.columns if name.startswith("GR")]) \
            .distinct()
        raw_data_for_add = raw_data.where(raw_data.PHA.isNotNull()) \
            .orderBy(raw_data.Year.desc()) \
            .withColumnRenamed("City_Tier_2010", "CITYGROUP") \
            .join(growth_rate, on=["S_Molecule_for_gr", "CITYGROUP"], how="left")
        raw_data_for_add.persist()
    
        # 4.2 补充各个医院缺失的月份数据:
        # add_data
        # 原始数据的 PHA-Month-Year
        # original_range = raw_data_for_add.select("Year", "Month", "PHA").distinct()
    
        years = raw_data_for_add.select("Year").distinct() \
            .orderBy(raw_data_for_add.Year) \
            .toPandas()["Year"].values.tolist()
        
        original_range = original_range_raw.where(original_range_raw.Year.isin(years))
        # print(years)
    
        growth_rate_index = [index for index, name in enumerate(raw_data_for_add.columns) if name.startswith("GR")]
        # print(growth_rate_index)
    
        # 对每年的缺失数据分别进行补数
        empty = 0
        for eachyear in years:
            # cal_time_range
            # 当前年：月份-PHA
            current_range_pha_month = original_range.where(original_range.Year == eachyear) \
                .select("Month", "PHA").distinct()
            # 当前年：月份
            current_range_month = current_range_pha_month.select("Month").distinct()
            # 其他年：月份-当前年有的月份，PHA-当前年没有的医院
            other_years_range = original_range.where(original_range.Year != eachyear) \
                .join(current_range_month, on="Month", how="inner") \
                .join(current_range_pha_month, on=["Month", "PHA"], how="left_anti")
            # 其他年：与当前年的年份差值，比重计算
            other_years_range = other_years_range \
                .withColumn("time_diff", (other_years_range.Year - eachyear)) \
                .withColumn("weight", func.when((other_years_range.Year > eachyear), (other_years_range.Year - eachyear - 0.5)).
                            otherwise(other_years_range.Year * (-1) + eachyear))
            # 选择比重最小的年份：用于补数的 PHA-Month-Year
            current_range_for_add = other_years_range.withColumn("row_number", func.row_number().over(Window.partitionBy("PHA", "Month").orderBy( col('weight').asc() ))) \
                                        .where(col('row_number') == 1) \
                                        .select("PHA", "Month", "Year")
    
            # get_seed_data
            # 从 rawdata 根据 current_range_for_add 获取用于补数的数据
            current_raw_data_for_add = raw_data_for_add.where(raw_data_for_add.Year != eachyear) \
                .join(current_range_for_add, on=["Month", "PHA", "Year"], how="inner")
            current_raw_data_for_add = current_raw_data_for_add \
                .withColumn("time_diff", (current_raw_data_for_add.Year - eachyear)) \
                .withColumn("weight", func.when((current_raw_data_for_add.Year > eachyear), (current_raw_data_for_add.Year - eachyear - 0.5)).
                            otherwise(current_raw_data_for_add.Year * (-1) + eachyear))
    
            # cal_seed_with_gr
            # 当前年与(当前年+1)的增长率所在列的index
            base_index = eachyear - min(years) + min(growth_rate_index)
            current_raw_data_for_add = current_raw_data_for_add.withColumn("Sales_bk", current_raw_data_for_add.Sales)
    
            # 为补数计算增长率
            current_raw_data_for_add = current_raw_data_for_add \
                .withColumn("min_index", func.when((current_raw_data_for_add.Year < eachyear), (current_raw_data_for_add.time_diff + base_index)).
                            otherwise(base_index)) \
                .withColumn("max_index", func.when((current_raw_data_for_add.Year < eachyear), (base_index - 1)).
                            otherwise(current_raw_data_for_add.time_diff + base_index - 1)) \
                .withColumn("total_gr", func.lit(1))
    
            for i in growth_rate_index:
                col_name = current_raw_data_for_add.columns[i]
                current_raw_data_for_add = current_raw_data_for_add.withColumn(col_name, func.when((current_raw_data_for_add.min_index > i) | (current_raw_data_for_add.max_index < i), 1).
                                                             otherwise(current_raw_data_for_add[col_name]))
                current_raw_data_for_add = current_raw_data_for_add.withColumn(col_name, func.when(current_raw_data_for_add.Year > eachyear, current_raw_data_for_add[col_name] ** (-1)).
                                                             otherwise(current_raw_data_for_add[col_name]))
                current_raw_data_for_add = current_raw_data_for_add.withColumn("total_gr", current_raw_data_for_add.total_gr * current_raw_data_for_add[col_name])
    
            current_raw_data_for_add = current_raw_data_for_add.withColumn("final_gr", func.when(current_raw_data_for_add.total_gr < 2, current_raw_data_for_add.total_gr).
                                                         otherwise(2))
    
            # 为当前年的缺失数据补数：根据增长率计算 Sales，匹配 price，计算 Units=Sales/price
            current_adding_data = current_raw_data_for_add \
                .withColumn("Sales", current_raw_data_for_add.Sales * current_raw_data_for_add.final_gr) \
                .withColumn("Year", func.lit(eachyear))
            current_adding_data = current_adding_data.withColumn("year_month", current_adding_data.Year * 100 + current_adding_data.Month)
            current_adding_data = current_adding_data.withColumn("year_month", current_adding_data["year_month"].cast(DoubleType()))
    
            current_adding_data = current_adding_data.withColumnRenamed("CITYGROUP", "City_Tier_2010") \
                .join(price, on=["min2", "year_month", "City_Tier_2010"], how="inner") \
                .join(price_city, on=["min2", "year_month", "City", "Province"], how="left")
    
            current_adding_data = current_adding_data.withColumn('Price', func.when(current_adding_data.Price_city.isNull(), 
                                                                                    current_adding_data.Price_tier) \
                                                                             .otherwise(current_adding_data.Price_city))
    
            current_adding_data = current_adding_data.withColumn("Units", func.when(current_adding_data.Sales == 0, 0).
                                                         otherwise(current_adding_data.Sales / current_adding_data.Price)) \
                .na.fill({'Units': 0})
    
            if empty == 0:
                # adding_data = current_adding_data                
                current_adding_data = current_adding_data.repartition(1)
                current_adding_data.write.format("parquet") \
                    .mode("overwrite").save(adding_data_tmp_path)
            else:
                # adding_data = adding_data.union(current_adding_data)
                current_adding_data = current_adding_data.repartition(1)
                current_adding_data.write.format("parquet") \
                    .mode("append").save(adding_data_tmp_path)
            empty = empty + 1
            
        adding_data = spark.read.parquet(adding_data_tmp_path)
        
        return adding_data, original_range
    
    # 执行函数 add_data
    if monthly_update == "False" and if_add_data == "True":
        print('4 补数')
        # 补数：add_data
        add_data_out = add_data(raw_data, growth_rate)
        adding_data = add_data_out[0]
        original_range = add_data_out[1]
    
    elif monthly_update == "True" and if_add_data == "True":
        #published_left = spark.read.csv(published_left_path, header=True)
        #published_right = spark.read.csv(published_right_path, header=True)
        #not_arrived =  spark.read.csv(not_arrived_path, header=True)
    
        print('4 补数')
    
        for index, month in enumerate(range(first_month, current_month + 1)):
            # publish交集，去除当月未到
            # month_hospital = published_left.intersect(published_right) \
            #     .exceptAll(not_arrived.where(not_arrived.Date == current_year*100 + month).select("ID")) \
            #     .toPandas()["ID"].tolist()
    
            raw_data_month = raw_data.where(raw_data.Month == month)
    
            growth_rate_month = growth_rate.where(growth_rate.month_for_monthly_add == month)
    
            # 补数：add_data
            adding_data_monthly = add_data(raw_data_month, growth_rate_month)[0]
    
            # 输出adding_data
            if index == 0:
                # adding_data = adding_data_monthly
                adding_data_monthly = adding_data_monthly.repartition(1)
                adding_data_monthly.write.format("parquet") \
                    .mode("overwrite").save(adding_data_path)
            else:
                # adding_data = adding_data.union(adding_data_monthly)
                adding_data_monthly = adding_data_monthly.repartition(1)
                adding_data_monthly.write.format("parquet") \
                    .mode("append").save(adding_data_path)
            print("输出 adding_data：" + adding_data_path)
    
    
    if monthly_update == "False" and if_add_data == "True":
        adding_data = adding_data.repartition(2)
        adding_data.write.format("parquet") \
            .mode("overwrite").save(adding_data_path)
        print("输出 adding_data：" + adding_data_path)
    elif monthly_update == "True" and if_add_data == "True":
        adding_data = spark.read.parquet(adding_data_path)
    
    # 1.8 合并补数部分和原始部分:
    # combind_data
    if if_add_data == "True":
        raw_data_adding = (raw_data.withColumn("add_flag", func.lit(0))) \
            .union(adding_data.withColumn("add_flag", func.lit(1)).select(raw_data.columns + ["add_flag"]))
    else:
        raw_data_adding = raw_data.withColumn("add_flag", func.lit(0))
    raw_data_adding.persist()
    
    # 输出
    # raw_data_adding = raw_data_adding.repartition(2)
    # raw_data_adding.write.format("parquet") \
    #     .mode("overwrite").save(raw_data_adding_path)
    # print("输出 raw_data_adding：" + raw_data_adding_path)
    
    if monthly_update == "False":
        if if_add_data == "True":
            # 1.9 进一步为最后一年独有的医院补最后一年的缺失月（可能也要考虑第一年）:add_data_new_hosp
            years = original_range.select("Year").distinct() \
                .orderBy(original_range.Year) \
                .toPandas()["Year"].values.tolist()
    
            # 只在最新一年出现的医院
            new_hospital = (original_range.where(original_range.Year == max(years)).select("PHA").distinct()) \
                .subtract(original_range.where(original_range.Year != max(years)).select("PHA").distinct())
            # print("以下是最新一年出现的医院:" + str(new_hospital.toPandas()["PHA"].tolist()))
            # 输出
            new_hospital = new_hospital.repartition(2)
            new_hospital.write.format("parquet") \
                .mode("overwrite").save(new_hospital_path)
    
            print("输出 new_hospital：" + new_hospital_path)
    
            # 最新一年没有的月份
            missing_months = (original_range.where(original_range.Year != max(years)).select("Month").distinct()) \
                .subtract(original_range.where(original_range.Year == max(years)).select("Month").distinct())
    
            # 如果最新一年有缺失月份，需要处理
            if missing_months.count() == 0:
                print("missing_months=0")
                raw_data_adding_final = raw_data_adding
            else:
                number_of_existing_months = 12 - missing_months.count()
                # 用于groupBy的列名：raw_data_adding列名去除list中的列名
                group_columns = set(raw_data_adding.columns) \
                    .difference(set(['Month', 'Sales', 'Units', '季度', "sales_value__rmb_", "total_units", "counting_units", "year_month"]))
                # 补数重新计算
                adding_data_new = raw_data_adding \
                    .where(raw_data_adding.add_flag == 1) \
                    .where(raw_data_adding.PHA.isin(new_hospital["PHA"].tolist())) \
                    .groupBy(list(group_columns)).agg({"Sales": "sum", "Units": "sum"})
                adding_data_new = adding_data_new \
                    .withColumn("Sales", adding_data_new["sum(Sales)"] / number_of_existing_months) \
                    .withColumn("Units", adding_data_new["sum(Units)"] / number_of_existing_months) \
                    .crossJoin(missing_months)
                # 生成最终补数结果
                same_names = list(set(raw_data_adding.columns).intersection(set(adding_data_new.columns)))
                raw_data_adding_final = raw_data_adding.select(same_names) \
                    .union(adding_data_new.select(same_names))
        else:
            raw_data_adding_final = raw_data_adding
    elif monthly_update == "True":
        raw_data_adding_final = raw_data_adding \
        .where((raw_data_adding.Year == current_year) & (raw_data_adding.Month >= first_month) & (raw_data_adding.Month <= current_month) )
    
    # 输出补数结果 raw_data_adding_final
    raw_data_adding_final = raw_data_adding_final.repartition(2)
    raw_data_adding_final.write.format("parquet") \
        .mode("overwrite").save(raw_data_adding_final_path)
    
    print("输出 raw_data_adding_final：" + raw_data_adding_final_path)
    
    print('数据执行-Finish')




def job4_panel(kwargs):
  
    ### input args ###
    max_path = kwargs.get('max_path', 'D:/Auto_MAX/MAX/')
    project_name = kwargs['project_name']
    model_month_left = kwargs['model_month_left']
    model_month_right = kwargs['model_month_right']
    if_others = kwargs.get('if_others', 'False')
    current_year = kwargs.get('current_year', 'Empty')
    current_month = kwargs.get('current_month', 'Empty')
    paths_foradding = kwargs.get('paths_foradding', 'D:/Auto_MAX/MAX/Common_files/Not_arrived202001.csv, D:/Auto_MAX/MAX/Common_files/Not_arrived201912.csv, D:/Auto_MAX/MAX/Common_files/Unpublished2020.csv')
    not_arrived_path = kwargs.get('not_arrived_path', 'Empty')
    published_path = kwargs.get('published_path', 'Empty')
    panel_for_union = kwargs.get('panel_for_union', 'Empty')
    monthly_update = kwargs['monthly_update']
    out_path = kwargs.get('out_path', 'D:/Auto_MAX/MAX/')
    out_dir = kwargs['out_dir']
    add_47 = kwargs.get('add_47', 'False')
    time_left = kwargs['time_left']
    time_right = kwargs['time_right']
    source_type = kwargs.get('source_type', 'Empty')
    ### input args ###
    

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    import os
    import pandas as pd
    from pyspark.sql.functions import pandas_udf, PandasUDFType, udf, col    # %%
    # project_name = '京新'
    # out_dir = 'test'
    # monthly_update = "True"
    # model_month_left = "201901"
    # model_month_right = "201912"
    # all_models = "康复新液,益生菌,癫痫,他汀,帕金森,癫痫新分子"
    # universe_choice = "他汀:universe_他汀"
    # need_cleaning_cols = "Molecule, Brand, Form, Specifications, Pack_Number, Manufacturer, min1"
    # time_left = "202001"
    # time_right = "202004"
    # first_month = "1"
    # current_month = "4"
    # %%
    
    spark = SparkSession.builder.getOrCreate()
    
    print('job4_panel')
    
    if if_others == "True":
        out_dir = out_dir + "/others_box/"
    
    out_path_dir = out_path + "/" + project_name + '/' + out_dir
    # 输入
    universe_path = max_path + "/" + project_name + "/universe_base"
    market_path  = max_path + "/" + project_name + "/mkt_mapping"
    cpa_pha_path = max_path + "/" + project_name + "/cpa_pha_mapping"
    
    raw_data_adding_final_path = out_path_dir + "/raw_data_adding_final"
    new_hospital_path = out_path_dir  + "/new_hospital"
    if panel_for_union != "Empty":
        panel_for_union_path = out_path + "/" + project_name + '/' + panel_for_union
    else:
        panel_for_union_path = "Empty"
    
    if add_47 != "False" and add_47 != "True":
        logger.error('wrong input: add_47, False or True') 
        raise ValueError('wrong input: add_47, False or True')
        
    # 月更新相关输入
    if monthly_update != "False" and monthly_update != "True":
        logger.error('wrong input: monthly_update, False or True') 
        raise ValueError('wrong input: monthly_update, False or True')
    if monthly_update == "False":
        Notarrive_unpublished_paths = paths_foradding.replace(", ",",").split(",")
    elif monthly_update == "True":
        current_year = int(current_year)
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
        
    # 输出
    if if_others == "True":
        panel_path = out_path_dir + "/panel_result_box"
    else:
        panel_path = out_path_dir + "/panel_result"

    # %%
    # =========== 数据检查 =============
    print('数据检查-start')
    # 存储文件的缺失列
    misscols_dict = {}
    # universe file
    universe = spark.read.parquet(universe_path)
    colnames_universe = universe.columns
    misscols_dict.setdefault("universe", [])
    if ("Panel_ID" not in colnames_universe) and ("PHA" not in colnames_universe):
        misscols_dict["universe"].append("Panel_ID/PHA")
    # if ("Hosp_name" not in colnames_universe) and ("HOSP_NAME" not in colnames_universe):
    #     misscols_dict["universe"].append("Hosp_name/HOSP_NAME")
    if "City" not in colnames_universe:
        misscols_dict["universe"].append("City")
    if "Province" not in colnames_universe:
        misscols_dict["universe"].append("Province")
    # market file
    market = spark.read.parquet(market_path)
    colnames_market = market.columns
    misscols_dict.setdefault("market", [])
    if ("标准通用名" not in colnames_market) and ("通用名" not in colnames_market):
        misscols_dict["market"].append("标准通用名")
    if ("model" not in colnames_market) and ("mkt" not in colnames_market):
        misscols_dict["market"].append("model")
    # raw_data_adding_final file
    raw_data_adding_final = spark.read.parquet(raw_data_adding_final_path)
    colnames_adding = raw_data_adding_final.columns
    misscols_dict.setdefault("adding_data", [])
    colnamelist = ['PHA', 'City', 'ID', 'Molecule', 'Sales', 'Units', 'Province', 'Month', 'Year', 'min2', 'S_Molecule', 'add_flag']
    # 'min1', 'year_month', 'Brand', 'Form', 'Specifications', 'Pack_Number', 'Manufacturer', 'Source', 'Corp', 'Route', 'Path', 'Sheet', 'BI_hospital_code', 'City_Tier_2010',
    # "标准商品名", 'S_Molecule_for_gr',
    for each in colnamelist:
        if each not in colnames_adding:
            misscols_dict["adding_data"].append(each)
    # 判断输入文件是否有缺失列
    misscols_dict_final = {}
    for eachfile in misscols_dict.keys():
        if len(misscols_dict[eachfile]) != 0:
            misscols_dict_final[eachfile] = misscols_dict[eachfile]
    # 如果有缺失列，则报错，停止运行
    if misscols_dict_final:
        print('miss columns: %s' % (misscols_dict_final))
        raise ValueError('miss columns: %s' % (misscols_dict_final))
    print('数据检查-Pass')

    # %%
    # =========== 数据执行 =============
    print('数据执行-start')
    # read_universe
    universe = spark.read.parquet(universe_path)
    if "CITYGROUP" in universe.columns:
        universe = universe.withColumnRenamed("CITYGROUP", "City_Tier_2010")
    elif "City_Tier" in universe.columns:
        universe = universe.withColumnRenamed("City_Tier", "City_Tier_2010")
    universe = universe \
        .withColumnRenamed("Panel_ID", "PHA") \
        .withColumnRenamed("Hosp_name", "HOSP_NAME") \
        .withColumn("City_Tier_2010", universe["City_Tier_2010"].cast(StringType()))
    if "HOSP_NAME" not in universe.columns:
        universe = universe.withColumn("HOSP_NAME",func.lit("0"))
    universe = universe.select("PHA", "HOSP_NAME", "Province", "City").distinct()
    universe.persist()
    # 读取 market
    market = spark.read.parquet(market_path)
    markets = market.withColumnRenamed("标准通用名", "通用名") \
        .withColumnRenamed("model", "mkt") \
        .select("mkt", "通用名").distinct()
    # 读取 raw_data_adding_final
    raw_data_adding_final = spark.read.parquet(raw_data_adding_final_path)
    raw_data_adding_final.persist()
    # 生成 panel
    panel = raw_data_adding_final \
        .join(markets, raw_data_adding_final["S_Molecule"] == markets["通用名"], how="left") \
        .drop("Province", "City") \
        .join(universe, on="PHA", how="left") \
        .withColumn("Date", raw_data_adding_final.Year * 100 + raw_data_adding_final.Month)
    panel = panel \
        .groupBy("ID", "Date", "min2", "mkt", "HOSP_NAME", "PHA", "S_Molecule", "Province", "City", "add_flag") \
        .agg(func.sum("Sales").alias("Sales"), func.sum("Units").alias("Units"))
    # 对 panel 列名重新命名
    old_names = ["ID", "Date", "min2", "mkt", "HOSP_NAME", "PHA", "S_Molecule", "Province", "City", "add_flag"]
    new_names = ["ID", "Date", "Prod_Name", "DOI", "Hosp_name", "HOSP_ID", "Molecule", "Province", "City", "add_flag"]
    for index, name in enumerate(old_names):
        panel = panel.withColumnRenamed(name, new_names[index])
    panel = panel \
        .withColumn("Prod_CNAME", panel.Prod_Name) \
        .withColumn("Strength", panel.Prod_Name) \
        .withColumn("DOIE", panel.DOI) \
        .withColumn("Date", panel["Date"].cast(DoubleType()))
    # 拆分 panel_raw_data， panel_add_data
    panel_raw_data = panel.where(panel.add_flag == 0)
    panel_raw_data.persist()
    panel_add_data = panel.where(panel.add_flag == 1)
    panel_add_data.persist()
    original_Date_molecule = panel_raw_data.select("Date", "Molecule").distinct()
    original_Date_ProdName = panel_raw_data.select("Date", "Prod_Name").distinct()
    panel_add_data = panel_add_data \
        .join(original_Date_molecule, on=["Date", "Molecule"], how="inner") \
        .join(original_Date_ProdName, on=["Date", "Prod_Name"], how="inner")
    # new_hospital = pd.read_excel(new_hospital_path)
    if monthly_update == "False":
        new_hospital = spark.read.parquet(new_hospital_path)
        new_hospital = new_hospital.toPandas()["PHA"].tolist()
    # 生成 panel_filtered
    # 早于model所用时间（历史数据），用new_hospital补数;
    # 处于model所用时间（模型数据），不补数；
    # 晚于model所用时间（月更新数据），用unpublished和not arrived补数
    # 取消Sanofi AZ 特殊处理（20210506）
    city_list = [u'北京市', u'上海市', u'天津市', u'重庆市', u'广州市', u'深圳市', u'西安市', u'大连市', u'成都市', u'厦门市', u'沈阳市']
    Province_list = [u'河北省', u'福建省', u'河北', u"福建"]
    
    if monthly_update == "False":
        if project_name == u"贝达" or project_name == "Sanofi" or project_name == "AZ":
            panel_add_data = panel_add_data.where(panel_add_data.Molecule != u"奥希替尼")
    # 去除 city_list和 Province_list
    if add_47 == "False":
        panel_add_data = panel_add_data \
            .where(~panel_add_data.City.isin(city_list)) \
            .where(~panel_add_data.Province.isin(Province_list))
    
    if monthly_update == "False":
        panel_add_data_history = panel_add_data \
            .where(panel_add_data.HOSP_ID.isin(new_hospital)) \
            .where(panel_add_data.Date < int(model_month_left)) \
            .select(panel_raw_data.columns)
        panel_filtered = panel_raw_data.union(panel_add_data_history.select(panel_raw_data.columns))
    def deal_ID_length(df):
        # ID不足7位的补足0到6位
        # 国药诚信医院编码长度是7位数字，cpa医院编码是6位数字，其他还有包含字母的ID
        df = df.withColumn("ID", df["ID"].cast(StringType()))
        # 去掉末尾的.0
        df = df.withColumn("ID", func.regexp_replace("ID", "\\.0", ""))
        df = df.withColumn("ID", func.when(func.length(df.ID) < 7, func.lpad(df.ID, 6, "0")).otherwise(df.ID))
        return df

    if monthly_update == "True":
        cpa_pha_mapping = spark.read.parquet(cpa_pha_path)
        cpa_pha_mapping = deal_ID_length(cpa_pha_mapping).filter(cpa_pha_mapping["推荐版本"] == 1).select("ID", "PHA").distinct()
        # unpublished文件
        # unpublished 列表创建：published_left中有而published_right没有的ID列表，然后重复12次，时间为current_year*100 + i
        if source_type =='GYC':
            published_left = spark.read.csv(published_left_path, header=True).where(col('Source')=='GYC')
            published_right_raw = spark.read.csv(published_right_path, header=True).where(col('Source')=='GYC')
        elif source_type =='CPA':
            published_left = spark.read.csv(published_left_path, header=True).where(col('Source')=='CPA')
            published_right_raw = spark.read.csv(published_right_path, header=True).where(col('Source')=='CPA') 
        else:
            published_left = spark.read.csv(published_left_path, header=True)
            published_right_raw = spark.read.csv(published_right_path, header=True)
        
        published_left = published_left.select('ID').distinct()
        published_left = deal_ID_length(published_left).join(cpa_pha_mapping, on='ID', how='left').where(~col('PHA').isNull()).select('PHA').distinct()
        
        published_right_raw = published_right_raw.select('ID').distinct()
        published_right_raw = deal_ID_length(published_right_raw).join(cpa_pha_mapping, on='ID', how='left').where(~col('PHA').isNull())
        published_right = published_right_raw.select('PHA').distinct()
        
        unpublished_ID=published_left.subtract(published_right).toPandas()['PHA'].values.tolist()
        unpublished_ID_num=len(unpublished_ID)
        all_month=list(range(1,13,1))*unpublished_ID_num
        all_month.sort()
        unpublished_dict={"PHA":unpublished_ID*12,"Date":[current_year*100 + i for i in all_month]}

        df = pd.DataFrame(data=unpublished_dict)
        df = df[["PHA","Date"]]
        schema = StructType([StructField("PHA", StringType(), True), StructField("Date", StringType(), True)])
        unpublished = spark.createDataFrame(df, schema)
        unpublished = unpublished.select("PHA","Date")
        
        if source_type !='GYC':
            # not_arrive文件
            # published_right：cpa独有pha，not_arrive文件只保留这部分pha（用项目内的pha文件）
            published_right_raw = published_right_raw.withColumn("source", func.length('ID'))
            published_right_only_cpa = published_right_raw.where(col('source')==6).select('PHA').distinct().subtract(published_right_raw.where(col('source')==7).select('PHA').distinct())

            Notarrive = spark.read.csv(not_arrived_path, header=True)
            Notarrive = Notarrive.select("ID","Date")
            Notarrive = deal_ID_length(Notarrive).join(cpa_pha_mapping, on='ID', how='left').where(~col('PHA').isNull()).select("PHA","Date").distinct() \
                        .join(published_right_only_cpa, on='PHA', how='inner')
            
            # 合并unpublished和not_arrive文件
            Notarrive_unpublished = unpublished.union(Notarrive).distinct()
        else:
            Notarrive_unpublished = unpublished.distinct()
        
        future_range = Notarrive_unpublished.withColumn("Date", Notarrive_unpublished["Date"].cast(DoubleType()))
        panel_add_data_future = deal_ID_length(panel_add_data).where(panel_add_data.Date > int(model_month_right)) \
            .join(cpa_pha_mapping, on=["ID"], how="left")  \
            .join(future_range, on=["Date", "PHA"], how="inner") \
            .select(panel_raw_data.columns)
        panel_filtered = panel_raw_data.union(panel_add_data_future)
        # 与之前的panel结果合并
        if panel_for_union_path != "Empty":
            panel_for_union = spark.read.parquet(panel_for_union_path)
            panel_filtered =  panel_filtered.union(panel_for_union.select(panel_filtered.columns))
            
        

    # %%
    # panel_filtered.groupBy("add_flag").agg({"Sales": "sum"}).show()
    # panel_filtered.groupBy("add_flag").agg({"Sales": "sum"}).show()
    panel_filtered = panel_filtered.repartition(2)
    panel_filtered.write.format("parquet") \
        .mode("overwrite").save(panel_path)
        
    panel_filtered.where( (col('Date') >= time_left) & (col('Date') <= time_right) ) \
                .repartition(1).write.format("csv").option("header", "true") \
                .mode("overwrite").save(f"{panel_path}.csv")
                
    print("输出 panel_filtered 结果：" + panel_path)
    print('数据执行-Finish')




def job5_max_weight(kwargs):

    ### input args ###
    max_path = kwargs.get('max_path', 'D:/Auto_MAX/MAX/')
    project_name = kwargs['project_name']
    if_base = kwargs.get('if_base', 'False')
    time_left = kwargs['time_left']
    time_right = kwargs['time_right']
    left_models = kwargs.get('left_models', 'Empty')
    left_models_time_left = kwargs.get('left_models_time_left', 'Empty')
    right_models = kwargs.get('right_models', 'Empty')
    right_models_time_right = kwargs.get('right_models_time_right', 'Empty')
    all_models = kwargs['all_models']
    universe_choice = kwargs.get('universe_choice', 'Empty')
    if_others = kwargs.get('if_others', 'False')
    out_path = kwargs.get('out_path', 'D:/Auto_MAX/MAX/')
    out_dir = kwargs['out_dir']
    use_d_weight = kwargs.get('use_d_weight', 'Empty')
    ### input args ###

    from pyspark.sql import SparkSession
    import os
    import pandas as pd
    from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
    from pyspark.sql import functions as func
    from pyspark.sql.functions import col     # %%
    # project_name = '京新'
    # out_dir = 'test'
    # monthly_update = "True"
    # model_month_left = "201901"
    # model_month_right = "201912"
    # all_models = "他汀"
    # universe_choice = "他汀:universe_他汀"
    # need_cleaning_cols = "Molecule, Brand, Form, Specifications, Pack_Number, Manufacturer, min1"
    # time_left = "202001"
    # time_right = "202004"
    # first_month = "1"
    # current_month = "4"
    # %%
    
    spark = SparkSession.builder.getOrCreate()
    
    print('job5_max')
    # 输入输出
    if if_base == "False":
        if_base = False
    elif if_base == "True":
        if_base = True
    else:
        raise ValueError('if_base: False or True')
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
    time_parameters = [int(time_left), int(time_right), left_models, int(left_models_time_left), right_models, int(right_models_time_right)]
    if all_models != "Empty":
        all_models = all_models.replace(", ",",").split(",")
    else:
        all_models = []
    project_path = max_path + "/" + project_name
    if if_others == "True":
        out_dir = out_dir + "/others_box/"
    out_path_dir = out_path + "/" + project_name + '/' + out_dir
    
    if use_d_weight != "Empty":
        use_d_weight = use_d_weight.replace(" ","").split(",")
    else:
        use_d_weight = []

    # %%
    # 市场的universe文件
    universe_choice_dict={}
    if universe_choice != "Empty":
        for each in universe_choice.replace(", ",",").split(","):
            market_name = each.split(":")[0]
            universe_name = each.split(":")[1]
            universe_choice_dict[market_name]=universe_name
    # 医院权重文件     
    PHA_weight_path = max_path + "/" + project_name + '/PHA_weight'
    PHA_weight = spark.read.parquet(PHA_weight_path)   
    # 是否加上 weight_default
    if use_d_weight:
        PHA_weight_default_path = max_path + "/" + project_name + '/PHA_weight_default'
        PHA_weight_default = spark.read.parquet(PHA_weight_default_path)
        PHA_weight_default = PHA_weight_default.where(PHA_weight_default.DOI.isin(use_d_weight))
        PHA_weight_default = PHA_weight_default.withColumnRenamed('Weight', 'Weight_d')
        PHA_weight = PHA_weight.join(PHA_weight_default, on=['Province', 'City', 'DOI', 'PHA'], how='full')
        PHA_weight = PHA_weight.withColumn('Weight', func.when(col('Weight').isNull(), col('Weight_d')).otherwise(col('Weight')))
    
    PHA_weight = PHA_weight.select('Province', 'City', 'DOI', 'Weight', 'PHA')
    PHA_weight = PHA_weight.withColumnRenamed('Province', 'Province_w') \
                            .withColumnRenamed('City', 'City_w')

    # %%
    # 计算max 函数
    def calculate_max(market, if_base=False, if_box=False):
        print('market:' + market)
        # =========== 输入 =============
        # 根据 market 选择 universe 文件：choose_uni
        if market in universe_choice_dict.keys():
            universe_path = project_path + '/' + universe_choice_dict[market]
        else:
            universe_path = project_path + '/universe_base'
        # universe_outlier_path 以及 factor_path 文件选择
        universe_outlier_path = project_path + "/universe/universe_ot_" + market
        if if_base:
            factor_path = project_path + "/factor/factor_base"
        else:
            factor_path = project_path + "/factor/factor_" + market
        # panel 文件选择与读取 获得 original_panel
        panel_box_path = out_path_dir + "/panel_result_box"
        panel_path = out_path_dir + "/panel_result"
        if if_box:
            original_panel_path = panel_box_path
        else:
            original_panel_path = panel_path
        PHA_weight_market = PHA_weight.where(PHA_weight.DOI == market)
        # =========== 数据检查 =============
        print('数据检查-start')
        # 存储文件的缺失列
        misscols_dict = {}
        # universe file
        universe = spark.read.parquet(universe_path)
        colnames_universe = universe.columns
        misscols_dict.setdefault("universe", [])
        if ("City_Tier" not in colnames_universe) and ("CITYGROUP" not in colnames_universe) and ("City_Tier_2010" not in colnames_universe):
            misscols_dict["universe"].append("City_Tier/CITYGROUP")
        if ("Panel_ID" not in colnames_universe) and ("PHA" not in colnames_universe):
            misscols_dict["universe"].append("Panel_ID/PHA")
        # if ("Hosp_name" not in colnames_universe) and ("HOSP_NAME" not in colnames_universe):
        #     misscols_dict["universe"].append("Hosp_name/HOSP_NAME")
        if "PANEL" not in colnames_universe:
            misscols_dict["universe"].append("PANEL")
        if "BEDSIZE" not in colnames_universe:
            misscols_dict["universe"].append("BEDSIZE")
        if "Seg" not in colnames_universe:
            misscols_dict["universe"].append("Seg")
        # universe_outlier file
        universe_outlier = spark.read.parquet(universe_outlier_path)
        colnames_universe_outlier = universe_outlier.columns
        misscols_dict.setdefault("universe_outlier", [])
        if ("City_Tier" not in colnames_universe) and ("CITYGROUP" not in colnames_universe) and ("City_Tier_2010" not in colnames_universe):
            misscols_dict["universe_outlier"].append("City_Tier/CITYGROUP")
        if ("Panel_ID" not in colnames_universe) and ("PHA" not in colnames_universe):
            misscols_dict["universe_outlier"].append("Panel_ID/PHA")
        # if ("Hosp_name" not in colnames_universe) and ("HOSP_NAME" not in colnames_universe):
        #     misscols_dict["universe_outlier"].append("Hosp_name/HOSP_NAME")
        if "PANEL" not in colnames_universe:
            misscols_dict["universe_outlier"].append("PANEL")
        if "BEDSIZE" not in colnames_universe:
            misscols_dict["universe_outlier"].append("BEDSIZE")
        if "Seg" not in colnames_universe:
            misscols_dict["universe_outlier"].append("Seg")
        if "Est_DrugIncome_RMB" not in colnames_universe:
            misscols_dict["universe_outlier"].append("Est_DrugIncome_RMB")
        # factor file
        factor = spark.read.parquet(factor_path)
        colnames_factor = factor.columns
        misscols_dict.setdefault("factor", [])
        if ("factor_new" not in colnames_factor) and ("factor" not in colnames_factor):
            misscols_dict["factor"].append("factor")
        if "City" not in colnames_factor:
            misscols_dict["factor"].append("City")
        # original_panel file
        original_panel = spark.read.parquet(original_panel_path)
        colnames_original_panel = original_panel.columns
        misscols_dict.setdefault("original_panel", [])
        colnamelist = ["DOI", 'HOSP_ID', 'Province', 'City', 'Date', 'Molecule', 'Prod_Name', 'Sales', 'Units']
        for each in colnamelist:
            if each not in colnames_original_panel:
                misscols_dict["original_panel"].append(each)
        # 判断输入文件是否有缺失列
        misscols_dict_final = {}
        for eachfile in misscols_dict.keys():
            if len(misscols_dict[eachfile]) != 0:
                misscols_dict_final[eachfile] = misscols_dict[eachfile]
        # 如果有缺失列，则报错，停止运行
        if misscols_dict_final:
            print('miss columns: %s' % (misscols_dict_final))
            raise ValueError('miss columns: %s' % (misscols_dict_final))
        print('数据检查-Pass')
        # =========== 数据执行 =============
        print('数据执行-start')
        # 选择 market 的时间范围：choose_months
        time_left = time_parameters[0]
        time_right = time_parameters[1]
        left_models = time_parameters[2]
        left_models_time_left = time_parameters[3]
        right_models = time_parameters[4]
        right_models_time_right = time_parameters[5]
        if market in left_models:
            time_left = left_models_time_left
        if market in right_models:
            time_right = right_models_time_right
        time_range = str(time_left) + '_' + str(time_right)
        # universe_outlier 文件读取与处理：read_uni_ot
        # universe_outlier = spark.read.parquet(universe_outlier_path)
        if "CITYGROUP" in universe_outlier.columns:
            universe_outlier = universe_outlier.withColumnRenamed("CITYGROUP", "City_Tier_2010")
        elif "City_Tier" in universe_outlier.columns:
            universe_outlier = universe_outlier.withColumnRenamed("City_Tier", "City_Tier_2010")
        universe_outlier = universe_outlier.withColumnRenamed("Panel_ID", "PHA") \
            .withColumnRenamed("Hosp_name", "HOSP_NAME")
        universe_outlier = universe_outlier.withColumn("City_Tier_2010", universe_outlier["City_Tier_2010"].cast(StringType()))
        universe_outlier = universe_outlier.select("PHA", "Est_DrugIncome_RMB", "PANEL", "Seg", "BEDSIZE")
        # universe 文件读取与处理：read_universe
        # universe = spark.read.parquet(universe_path)
        if "CITYGROUP" in universe.columns:
            universe = universe.withColumnRenamed("CITYGROUP", "City_Tier_2010")
        elif "City_Tier" in universe.columns:
            universe = universe.withColumnRenamed("City_Tier", "City_Tier_2010")
        universe = universe.withColumnRenamed("Panel_ID", "PHA") \
            .withColumnRenamed("Hosp_name", "HOSP_NAME")
        universe = universe.withColumn("City_Tier_2010", universe["City_Tier_2010"].cast(StringType()))
        # panel 文件读取 获得 original_panel
        # original_panel = spark.read.parquet(original_panel_path)
        original_panel = original_panel.where((original_panel.DOI == market) & (original_panel.Date >= time_left) & (original_panel.Date <= time_right)).cache() # TEST
        # 获得 panel, panel_seg：group_panel_by_seg
        # panel：整理成max的格式，包含了所有在universe的panel列标记为1的医院，当作所有样本医院的max
        universe_panel_all = universe.where(universe.PANEL == 1).select('PHA', 'BEDSIZE', 'PANEL', 'Seg')
        panel = original_panel \
            .join(universe_panel_all, original_panel.HOSP_ID == universe_panel_all.PHA, how="inner") \
            .groupBy('PHA', 'Province', 'City', 'Date', 'Molecule', 'Prod_Name', 'BEDSIZE', 'PANEL', 'Seg') \
            .agg(func.sum("Sales").alias("Predict_Sales"), func.sum("Units").alias("Predict_Unit")).cache()
        # panel_seg：整理成seg层面，包含了所有在universe_ot的panel列标记为1的医院，可以用来得到非样本医院的max
        panel_drugincome = universe_outlier.where(universe_outlier.PANEL == 1) \
            .groupBy("Seg") \
            .agg(func.sum("Est_DrugIncome_RMB").alias("DrugIncome_Panel")).cache() # TEST
        original_panel_tmp = original_panel.join(universe_outlier, original_panel.HOSP_ID == universe_outlier.PHA, how='left').cache() # TEST
        panel_seg = original_panel_tmp.where(original_panel_tmp.PANEL == 1) \
            .groupBy('Date', 'Prod_Name', 'Seg', 'Molecule') \
            .agg(func.sum("Sales").alias("Sales_Panel"), func.sum("Units").alias("Units_Panel")).cache()
        panel_seg = panel_seg.join(panel_drugincome, on="Seg", how="left").cache() # TEST
        # *** PHA_city 权重计算
        original_panel_weight = original_panel_tmp.join(PHA_weight_market, on=['PHA'], how='left')
        original_panel_weight = original_panel_weight.withColumn('Weight', func.when(original_panel_weight.Weight.isNull(), func.lit(1)) \
                                                                                .otherwise(original_panel_weight.Weight))
        original_panel_weight = original_panel_weight.withColumn('Sales_w', original_panel_weight.Sales * original_panel_weight.Weight) \
                                                    .withColumn('Units_w', original_panel_weight.Units * original_panel_weight.Weight)
        panel_seg_weight = original_panel_weight.where(original_panel_weight.PANEL == 1) \
            .groupBy('Date', 'Prod_Name', 'Seg', 'Molecule', 'Province_w', 'City_w') \
            .agg(func.sum("Sales_w").alias("Sales_Panel_w"), func.sum("Units_w").alias("Units_Panel_w")).cache() # TEST
        panel_seg_weight = panel_seg_weight.join(panel_drugincome, on="Seg", how="left").cache() # TEST
        panel_seg_weight = panel_seg_weight.withColumnRenamed('Province_w', 'Province') \
                        .withColumnRenamed('City_w', 'City')
        # 将非样本的segment和factor等信息合并起来：get_uni_with_factor
        # factor = spark.read.parquet(factor_path)
        if "factor" not in factor.columns:
            factor = factor.withColumnRenamed("factor_new", "factor")
            
        if 'Province' in factor.columns:
            factor = factor.select('City', 'factor', 'Province').distinct()
            universe_factor_panel = universe.join(factor, on=["City", 'Province'], how="left").cache() # TEST
        else:
            factor = factor.select('City', 'factor').distinct()
            universe_factor_panel = universe.join(factor, on=["City"], how="left").cache() # TEST
            
        universe_factor_panel = universe_factor_panel \
            .withColumn("factor", func.when(func.isnull(universe_factor_panel.factor), func.lit(1)).otherwise(universe_factor_panel.factor)) \
            .where(universe_factor_panel.PANEL == 0) \
            .select('Province', 'City', 'PHA', 'Est_DrugIncome_RMB', 'Seg', 'BEDSIZE', 'PANEL', 'factor').cache() # TEST
        # 为这些非样本医院匹配上样本金额、产品、年月、所在segment的drugincome之和
        # 优先有权重的结果
        max_result = universe_factor_panel.join(panel_seg, on="Seg", how="left")
        max_result = max_result.join(panel_seg_weight.select('Date', 'Prod_Name', 'Molecule', 'Seg', 'Province', 'City', 'Sales_Panel_w', 'Units_Panel_w').distinct(), 
                                        on=['Date', 'Prod_Name', 'Molecule', 'Seg', 'Province', 'City'], how="left")
        max_result = max_result.withColumn('Sales_Panel', func.when(max_result.Sales_Panel_w.isNull(), max_result.Sales_Panel) \
                                                                .otherwise(max_result.Sales_Panel_w)) \
                                .withColumn('Units_Panel', func.when(max_result.Units_Panel_w.isNull(), max_result.Units_Panel) \
                                                                .otherwise(max_result.Units_Panel_w)) \
                                .drop('Sales_Panel_w', 'Units_Panel_w')
        # 预测值等于样本金额乘上当前医院drugincome再除以所在segment的drugincome之和
        max_result = max_result.withColumn("Predict_Sales", (max_result.Sales_Panel / max_result.DrugIncome_Panel) * max_result.Est_DrugIncome_RMB) \
            .withColumn("Predict_Unit", (max_result.Units_Panel / max_result.DrugIncome_Panel) * max_result.Est_DrugIncome_RMB).cache() # TEST
        # 为什么有空，因为部分segment无样本或者样本金额为0：remove_nega
        max_result = max_result.where(~func.isnull(max_result.Predict_Sales))
        max_result = max_result.withColumn("positive", func.when(max_result["Predict_Sales"] > 0, 1).otherwise(0))
        max_result = max_result.withColumn("positive", func.when(max_result["Predict_Unit"] > 0, 1).otherwise(max_result.positive))
        max_result = max_result.where(max_result.positive == 1).drop("positive")
        # 乘上factor
        max_result = max_result.withColumn("Predict_Sales", max_result.Predict_Sales * max_result.factor) \
            .withColumn("Predict_Unit", max_result.Predict_Unit * max_result.factor) \
            .select('PHA', 'Province', 'City', 'Date', 'Molecule', 'Prod_Name', 'BEDSIZE', 'PANEL',
                    'Seg', 'Predict_Sales', 'Predict_Unit')
        # 合并样本部分
        max_result = max_result.union(panel.select(max_result.columns))
        # 输出结果
        # if if_base == False:
        max_result = max_result.repartition(2)
        if if_box:
            max_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + '_'  + market + "_hosp_level_box"
            max_result.write.format("parquet") \
                .mode("overwrite").save(max_path)
        else:
            max_path = out_path_dir + "/MAX_result/MAX_result_" + time_range + '_' + market + "_hosp_level"
            max_result.write.format("parquet") \
                .mode("overwrite").save(max_path)
        print('数据执行-Finish')

    # %%
    # 执行函数
    if if_others == "False":
        for i in all_models:
            calculate_max(i, if_base=if_base, if_box=False)
    elif if_others == "True":
        for i in all_models:
            calculate_max(i, if_base=if_base, if_box=True)




def job6_max_city(kwargs):
 
    ### input args ###
    max_path = kwargs.get('max_path', 'D:/Auto_MAX/MAX/')
    project_name = kwargs['project_name']
    time_left = kwargs['time_left']
    time_right = kwargs['time_right']
    left_models = kwargs.get('left_models', 'Empty')
    left_models_time_left = kwargs.get('left_models_time_left', 'Empty')
    right_models = kwargs.get('right_models', 'Empty')
    right_models_time_right = kwargs.get('right_models_time_right', 'Empty')
    all_models = kwargs['all_models']
    if_others = kwargs.get('if_others', 'False')
    out_path = kwargs.get('out_path', 'D:/Auto_MAX/MAX/')
    out_dir = kwargs['out_dir']
    minimum_product_columns = kwargs.get('minimum_product_columns', 'Brand, Form, Specifications, Pack_Number, Manufacturer')
    minimum_product_sep = kwargs.get('minimum_product_sep', '|')
    minimum_product_newname = kwargs.get('minimum_product_newname', 'min1')
    if_two_source = kwargs.get('if_two_source', 'False')
    hospital_level = kwargs.get('hospital_level', 'False')
    bedsize = kwargs.get('bedsize', 'True')
    id_bedsize_path = kwargs.get('id_bedsize_path', 'Empty')
    for_nh_model = kwargs.get('for_nh_model', 'False')
    ### input args ###


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
    
    spark = SparkSession.builder.getOrCreate()
    print('job6')
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

        for i in minimum_product_columns[1:]:
            raw_data = raw_data.withColumn(i, raw_data[i].cast(StringType()))
            raw_data = raw_data.withColumn("tmp", func.concat(
                raw_data["tmp"],
                func.lit(minimum_product_sep),
                func.when(func.isnull(raw_data[i]), func.lit("NA")).otherwise(raw_data[i])))

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
    max_result_city_all = max_result_all.union(raw_data_city.select(max_result_all.columns))

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
    
    if hospital_level == "False" and bedsize == "True":
        file_name = max_result_city_path.replace('//', '/').split('s3:/ph-max-auto/')[1]
    else:
        file_name = max_result_city_csv_path.replace('//', '/').split('s3:/ph-max-auto/')[1]

    s3 = boto3.resource('s3', region_name='cn-northwest-1')
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
    '''
    max_result_city_final = max_result_city

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
    print('数据执行-Finish')



