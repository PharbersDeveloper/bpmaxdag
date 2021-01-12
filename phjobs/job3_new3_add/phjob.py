# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.
This is job template for Pharbers Max Job
"""

import pandas as pd
from phcli.ph_logs.ph_logs import phs3logger
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
from pyspark.sql.functions import pandas_udf, PandasUDFType

import pandas as pd
import numpy as np
from scipy.spatial import distance
import math
import json

def execute(max_path, project_name, out_path, out_dir, model_month_right, model_month_left, current_year, current_month, first_month):
    logger = phs3logger()
    os.environ["PYSPARK_PYTHON"] = "python3"
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data from s3") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "1") \
        .config("spark.executor.memory", "2g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .config("spark.sql.execution.arrow.enabled", "false") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", 10000) \
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
    
    
    out_path_dir = out_path + "/" + project_name + '/' + out_dir
    
    # 输入
    df_sales_path = out_path_dir + "/New_data_add_Out/df_sales"
    df_units_path = out_path_dir + "/New_data_add_Out/df_units"
    
    product_map_path = out_path_dir + '/prod_mapping'
    universe_path = out_path + "/" + project_name + '/universe_base'
    cpa_pha_mapping_path = out_path + "/" + project_name + '/cpa_pha_mapping'
    MNF_path = max_path  + "/Common_files/MNF_TYPE_PFC"
    VBP_path = max_path  + "/Common_files/VBP_pfc_molecule"
    
    df_near_hosp_non_vbp_sku_path = out_path_dir + "/New_data_add_Out/df_near_hosp_non_vbp_sku"
    df_near_hosp_mnc_sku_path = out_path_dir + "/New_data_add_Out/df_near_hosp_mnc_sku"
    
    current_year = int(current_year)
    current_month = int(current_month)
    first_month = int(first_month)
    model_month_right = int(model_month_right)
    model_month_left = int(model_month_left)
    
    model_month_colnames = [("Date" + str(i)) for i in list(range(model_month_left, model_month_right + 1))]
    
    
    # 输出
    result_ma_tmp_1_path = out_path_dir + "/New_data_add_Out/result_ma_tmp_1"
    result_ma_tmp_2_path = out_path_dir + "/New_data_add_Out/result_ma_tmp_2"
    result_ma_path = out_path_dir + "/New_data_add_Out/result_ma"
    result_non_vbp_path = out_path_dir + "/New_data_add_Out/result_non_vbp"
    result_mnc_path = out_path_dir + "/New_data_add_Out/result_mnc"
    result_vbp_path = out_path_dir + "/New_data_add_Out/result_vbp"
    
    
    # =============== 数据执行 =================
    
    #%% Read-in 
    df_sales = spark.read.parquet(df_sales_path)
    df_units = spark.read.parquet(df_units_path)
    
    df_sales = df_sales.select([func.col(col).alias(col + "_Sales") for col in df_sales.columns]) \
                    .withColumnRenamed("ID_Sales", "ID") \
                    .withColumnRenamed("pfc_Sales", "pfc")
    df_units = df_units.select([func.col(col).alias(col + "_Units") for col in df_units.columns]) \
                    .withColumnRenamed("ID_Units", "ID") \
                    .withColumnRenamed("pfc_Units", "pfc")
    
    df = df_sales.join(df_units, on=['ID','pfc'], how = 'inner')
    
    
    # universe
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
    hosp_info = universe.select('PHA', 'HOSP_NAME', 'Province', 'City')
    
    
    # id不足7位的补足为6位
    def distinguish_cpa_gyc(col, gyc_hospital_id_length):
        # gyc_hospital_id_length是国药诚信医院编码长度，一般是7位数字，cpa医院编码一般是6位数字。医院编码长度可以用来区分cpa和gyc
        return (func.length(col) < gyc_hospital_id_length)
        
    # cpa_pha
    cpa_pha_mapping = spark.read.parquet(cpa_pha_mapping_path)
    cpa_pha_mapping = cpa_pha_mapping.where(cpa_pha_mapping["推荐版本"] == "1").select('ID','PHA').distinct()
    cpa_pha_mapping = cpa_pha_mapping.withColumn("ID", cpa_pha_mapping["ID"].cast(IntegerType()))
    cpa_pha_mapping = cpa_pha_mapping.withColumn("ID", cpa_pha_mapping["ID"].cast(StringType()))
    cpa_pha_mapping = cpa_pha_mapping.withColumn("ID", 
                                func.when(distinguish_cpa_gyc(cpa_pha_mapping.ID, 7), 
                                func.lpad(cpa_pha_mapping.ID, 6, "0")).otherwise(cpa_pha_mapping.ID))
                                    
    # MNF, VBP
    mnf_map = spark.read.parquet(MNF_path)
    mnf_map = mnf_map.withColumn("pfc", mnf_map["pfc"].cast(IntegerType()))
    
    vbp_map = spark.read.parquet(VBP_path)
    vbp_map = vbp_map.select('pfc','药品通用名_标准').distinct() \
                .withColumnRenamed("药品通用名_标准", "通用名") \
                .withColumn("pfc", vbp_map["pfc"].cast(IntegerType()))
    
    # prod_map
    product_map = spark.read.parquet(product_map_path)
    if project_name == "Sanofi" or project_name == "AZ":
        product_map = product_map.withColumnRenamed(product_map.columns[21], "pfc")
    for col in product_map.columns:
        if col in ["标准通用名", "通用名_标准", "药品名称_标准", "S_Molecule_Name"]:
            product_map = product_map.withColumnRenamed(col, "通用名")
        if col in ["packcode", "Pack_ID", "Pack_Id", "PackID", "packid"]:
            product_map = product_map.withColumnRenamed(col, "pfc")
    product_map = product_map.select('pfc','通用名').distinct() \
                .withColumn("pfc", product_map["pfc"].cast(IntegerType()))
    
    #%% 匹配各种信息
    data_prod = df.join(cpa_pha_mapping, on='ID', how = 'left')
    data_prod = data_prod.join(hosp_info, on=["PHA"], how = 'left') \
            .join(mnf_map,  on = 'pfc', how = 'left') \
            .join(product_map, on='pfc', how = 'left')
            
    vbp_map_pfc = vbp_map.select("pfc").distinct().toPandas()["pfc"].tolist()        
    data_prod = data_prod.withColumn("VBP_prod", func.when(data_prod.pfc.isin(vbp_map_pfc), func.lit("True")).otherwise(func.lit("False")))
    vbp_map_molecule = vbp_map.select("通用名").distinct().toPandas()["通用名"].tolist()
    data_prod = data_prod.withColumn("VBP_mole", func.when(data_prod["通用名"].isin(vbp_map_molecule), func.lit("True")).otherwise(func.lit("False")))
    
    data_prod = data_prod.withColumn("Province" , \
                func.when(data_prod.City.isin('大连市','沈阳市','西安市','厦门市','广州市', '深圳市','成都市'), data_prod.City). \
                otherwise(data_prod.Province))
                
    #%% 拆分数据
    data_non_vbp = data_prod.where(data_prod.VBP_mole == "False") \
                .drop(*[i for i in data_prod.columns if "Units" in i])
    for eachcol in data_non_vbp.columns:
        if "_Sales" in eachcol:
            data_non_vbp = data_non_vbp.withColumnRenamed(eachcol, eachcol.replace("_Sales", ""))
    
    data_vbp = data_prod.where(data_prod.VBP_mole == "True") \
                .drop(*[i for i in data_prod.columns if "Sales" in i])
    for eachcol in data_vbp.columns:
        if "_Units" in eachcol:
            data_vbp = data_vbp.withColumnRenamed(eachcol, eachcol.replace("_Units", ""))
    
    data_mnc = data_vbp.where(data_vbp.MNF_TYPE != 'L')
    
    data_local = data_vbp.where(data_vbp.MNF_TYPE == 'L')   
    
    
    '''
    PART 2
    生成近邻名单，并且保存全局
    '''
    
    # 读取，转为字典格式
    
    #  data_non_vbp
    df_near_hosp_non_vbp_sku = spark.read.parquet(df_near_hosp_non_vbp_sku_path)
    
    # 转化为字典格式
    df_near_hosp_non_vbp_sku = df_near_hosp_non_vbp_sku.groupBy().agg(func.collect_list('dict').alias('dict_all')).select("dict_all").toPandas()
    df_near_hosp_non_vbp_sku = df_near_hosp_non_vbp_sku["dict_all"].values[0]
    length_dict = len(df_near_hosp_non_vbp_sku)
    
    dict_near_hosp_non_vbp_sku = ""
    for index, each in enumerate(df_near_hosp_non_vbp_sku):
        if index == 0:
            dict_near_hosp_non_vbp_sku += "{" + each[1:-1]
        elif index == length_dict - 1:
            dict_near_hosp_non_vbp_sku += "," + each[1:-1] + "}"
        else:
            dict_near_hosp_non_vbp_sku += "," + each[1:-1]
    near_hosp_non_vbp_sku  = json.loads(dict_near_hosp_non_vbp_sku)
    logger.info("near_hosp_non_vbp_sku")
    
    # data_mnc
    df_near_hosp_mnc_sku = spark.read.parquet(df_near_hosp_mnc_sku_path)
    
    # 转化为字典格式
    df_near_hosp_mnc_sku = df_near_hosp_mnc_sku.groupBy().agg(func.collect_list('dict').alias('dict_all')).select("dict_all").toPandas()
    df_near_hosp_mnc_sku = df_near_hosp_mnc_sku["dict_all"].values[0]
    length_dict = len(df_near_hosp_mnc_sku)
    
    dict_near_hosp_mnc_sku = ""
    for index, each in enumerate(df_near_hosp_mnc_sku):
        if index == 0:
            dict_near_hosp_mnc_sku += "{" + each[1:-1]
        elif index == length_dict - 1:
            dict_near_hosp_mnc_sku += "," + each[1:-1] + "}"
        else:
            dict_near_hosp_mnc_sku += "," + each[1:-1]
    near_hosp_mnc_sku  = json.loads(dict_near_hosp_mnc_sku)
    
    logger.info("near_hosp_mnc_sku")
    
    '''
    PART 3
    补充数据
    '''
    
    # =========== MovAvg ===========
    
    # %% Moving average先补一批non-VBP
    def pandas_udf_MovAvg_func(data_mole, level, date, near_hosp_sku):
        import pandas as pd
        import numpy as np
        from scipy.spatial import distance
        import math
        import json
        # import sys
        # reload(sys)
        # sys.setdefaultencoding('utf-8')
        
        # 分组后的 level
        level_name = data_mole[level][0]
        near_hosp = near_hosp_sku[level_name]
        
        date_list_num = []
        # 这里直接把年份里的月都表示出来吧
        for y in range((date - 4) // 100, date // 100 + 1):
            date_list_num += list(range(y * 100 + 1, y * 100 + 12 + 1))
        date_list = ['Date' + str(d) for d in date_list_num]
        # Top1医院是在每个政策区域的每个SKU上都有一个，并且必须和NN同步
        current_col = 'Date' + str(date)
        his_col = date_list[date_list.index(current_col) - 3:date_list.index(current_col)]
        # 当月
        data_month = data_mole[['ID', level, current_col]]
        # 当月未到
        data_month_na = data_month.loc[data_month[current_col].isna(),
                                       ['ID', level, current_col]]. \
            reset_index(drop=True)
        # 未到前三月
        data_month_na_his = data_mole.loc[data_mole[current_col].isna(), his_col]. \
            reset_index(drop=True)
        for i in range(len(data_month_na)):
            na_id = data_month_na.loc[i].ID
            na_min = data_month_na.loc[i][level]
            if (len(near_hosp[na_id]['NN']) < 3) | (na_id in near_hosp['TOP1']):
                data_month_na.loc[i, current_col] = data_month_na_his.loc[i]. \
                    mean(skipna=True)
                
        data_month = pd.concat([data_month, data_month_na])
        data_month = data_month[~data_month[current_col].isna()]
        
        data_month[['ID', level]] = data_month[['ID', level]].astype("str")
        return data_month
        
    
    def MovAvg(data, level, date, near_hosp_sku):
        # Top1医院是在每个政策区域的每个SKU上都有一个，并且必须和NN同步
        # 分组计算
        current_date = 'Date' + str(date)
        schema= StructType([
            StructField("ID", StringType(), True),
            StructField(level, StringType(), True),
            StructField(current_date, DoubleType(), True)
            ])
        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)    
        def pandas_udf_MovAvg(df):
            return pandas_udf_MovAvg_func(df, level, date, near_hosp_sku)
        # 输出 level，dict
        result = data.groupby([level]).apply(pandas_udf_MovAvg)
        return result
    
    
    #%% ma
    ## 循环写出result_ma_tmp，否则内存溢出
    data_non_vbp = data_non_vbp.withColumn("pfc", data_non_vbp.pfc.cast(StringType()))
    for m in range(current_year*100 + first_month, current_year*100 + current_month + 1):
        if m == current_year*100 + first_month:
            result_ma = data_non_vbp
        else:
            if (m % 2) == 0:
                result_ma = spark.read.parquet(result_ma_tmp_1_path)
            else:
                result_ma = spark.read.parquet(result_ma_tmp_2_path)
        result_month = MovAvg(result_ma, 'pfc', m, near_hosp_non_vbp_sku)
        result_ma = result_ma.drop('Date' + str(m))
        result_ma = result_ma.join(result_month, on=['ID', 'pfc'], how='left')
        if (m % 2) == 0:
            result_ma.repartition(1).write.format("parquet") \
                .mode("overwrite").save(result_ma_tmp_2_path)
        else:
            result_ma.repartition(1).write.format("parquet") \
                .mode("overwrite").save(result_ma_tmp_1_path)
        
    result_ma = result_ma.repartition(2)
    result_ma.write.format("parquet") \
        .mode("overwrite").save(result_ma_path)
    result_ma = spark.read.parquet(result_ma_path)
    
    logger.info("result_ma")
    
    # =========== NHospImpute =========== 
    # %% nn
    
    # %% 补充数据的func: MA先于KNN
    def pandas_udf_NHospImpute_func(data_mole, level, date, near_hosp_sku, min_level):
        import pandas as pd
        import numpy as np
        from scipy.spatial import distance
        import math
        import json
        # import sys
        # reload(sys)
        # sys.setdefaultencoding('utf-8')
        # 分组后的 level
        level_name = str(data_mole[level][0])
        near_hosp = near_hosp_sku[level_name]
        # result = pd.DataFrame()
        current_col = 'Date' + str(date)
        data_month = data_mole[['ID', min_level, current_col]]
        data_month_na = data_month.loc[data_month[current_col].isna(),
                                       ['ID', min_level, current_col]]. \
            reset_index(drop=True)
        for i in range(len(data_month_na)):
            na_id = data_month_na.loc[i].ID
            na_min = data_month_na.loc[i][min_level]
            if (len(near_hosp[na_id]['NN']) == 3):
                nn_values = [np.hstack((data_month.loc[(data_month.ID == near_hosp[na_id]['NN'][c]) &
                                                       (data_month[min_level] == na_min),
                                                       current_col].values, np.nan))[0] for c in [0, 1, 2]]
                nn_values = [max(x, 0) for x in nn_values]
                nn_weights = [1 / w for w in near_hosp[na_id]['Dist']]
                nn_values_ma = np.ma.array(nn_values, mask=np.isnan(np.array(nn_values)))
                data_month_na.loc[i, current_col] = np.ma.average(nn_values_ma, weights=nn_weights) * 1.05
        data_month = pd.concat([data_month, data_month_na])
        data_month = data_month[~data_month[current_col].isna()]
        # result = pd.concat([result, data_month])
        # *** yyw *** 数据类型,int 报错
        data_month[['ID', level]] = data_month[['ID', level]].astype("str")
        return data_month
                    
                    
    def NHospImpute(data, level, date, near_hosp_non_vbp_sku):
        min_level = str(np.where('pfc' in data.columns, 'pfc', 'min2'))
        
        # 分组计算
        current_date = 'Date' + str(date)
        
        schema= StructType([
            StructField("ID", StringType(), True),
            StructField(level, StringType(), True),
            StructField(current_date, DoubleType(), True)
            ])
        @pandas_udf(schema, PandasUDFType.GROUPED_MAP)    
        def pandas_udf_MovAvg(df):
            return pandas_udf_NHospImpute_func(df, level, date, near_hosp_non_vbp_sku, min_level)
        # 输出 level，dict
        result = data.groupby([level]).apply(pandas_udf_MovAvg)
        return result
        
    # %% nn  
    result_non_vbp = data_non_vbp.select(['ID', 'pfc'] + model_month_colnames)
    for m in range(current_year*100 + first_month, current_year*100 + current_month + 1):
        result_month = NHospImpute(result_ma, 'pfc', m, near_hosp_non_vbp_sku)
        result_non_vbp = result_non_vbp.join(result_month, on=['ID', 'pfc'], how='left')
    result_non_vbp = result_non_vbp.fillna(0)    
    
    result_non_vbp = result_non_vbp.repartition(2)
    result_non_vbp.write.format("parquet") \
        .mode("overwrite").save(result_non_vbp_path)
    result_non_vbp = spark.read.parquet(result_non_vbp_path)
    
    logger.info("result_non_vbp")
    
    # %% MNC
    data_mnc = data_mnc.withColumn("pfc", data_mnc.pfc.cast(StringType()))
    result_mnc = data_mnc.select(['ID', 'pfc'] + model_month_colnames)
    for m in range(current_year*100 + first_month, current_year*100 + current_month + 1):
        result_month = NHospImpute(data_mnc, 'pfc', m, near_hosp_mnc_sku)
        result_mnc = result_mnc.join(result_month, on=['ID', 'pfc'], how='left')
        
    result_mnc = result_mnc.repartition(2)
    result_mnc.write.format("parquet") \
        .mode("overwrite").save(result_mnc_path)
    result_mnc = spark.read.parquet(result_mnc_path)
    
    # %% Local
    # *** yyw *** isnan 改为isnull
    result_local = data_local
    for i in range(current_year*100 + first_month, current_year*100 + current_month + 1):
        temp_date = result_local.columns
        current_index = temp_date.index('Date' + str(i))
        current_cols = 'Date' + str(i)
        past_cols = result_local.columns[current_index - 3:current_index]
        result_local = result_local.withColumn(current_cols, \
                                        func.when(func.isnull(result_local[current_cols]), \
                                        (result_local[past_cols[0]] + result_local[past_cols[1]] + result_local[past_cols[2]])/3) \
                                        .otherwise(result_local[current_cols]))
    result_vbp = result_mnc.union(result_local.select(result_mnc.columns)).fillna(0)
    
    result_vbp = result_vbp.repartition(2)
    result_vbp.write.format("parquet") \
        .mode("overwrite").save(result_vbp_path)
    
    logger.info("result_vbp")