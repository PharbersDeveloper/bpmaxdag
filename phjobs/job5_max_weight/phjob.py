# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phcli.ph_logs.ph_logs import phs3logger
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
from pyspark.sql.functions import col
import os

def execute(max_path, project_name, if_base, time_left, time_right, left_models, left_models_time_left, right_models, right_models_time_right,
all_models, universe_choice, if_others, out_path, out_dir, need_test, use_d_weight):
    logger = phs3logger()
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

    logger.info('job5_max')

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
        
    # 计算max 函数
    def calculate_max(market, if_base=False, if_box=False):

        logger.info('market:' + market)

        # =========== 输入 =============
        # 根据 market 选择 universe 文件：choose_uni
        if project_name == "Sanofi" or project_name == "AZ":
            if market in ['SNY6', 'SNY10', 'SNY12', 'SNY13', 'AZ12', 'AZ18', 'AZ21']:
                universe_path = project_path + '/universe_az_sanofi_onc'
            elif market in ['SNY5', 'SNY9', 'AZ10', 'AZ11', 'AZ15', 'AZ16', 'AZ14', 'AZ26', 'AZ24']:
                universe_path = project_path + '/universe_az_sanofi_mch'
            else:
                universe_path = project_path + '/universe_base'
        else:
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
        logger.info('数据检查-start')

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
            logger.error('miss columns: %s' % (misscols_dict_final))
            raise ValueError('miss columns: %s' % (misscols_dict_final))

        logger.info('数据检查-Pass')

        # =========== 数据执行 =============

        logger.info('数据执行-start')

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

        logger.info('数据执行-Finish')


    # 执行函数
    if if_others == "False":
        for i in all_models:
            calculate_max(i, if_base=if_base, if_box=False)
    elif if_others == "True":
        for i in all_models:
            calculate_max(i, if_base=if_base, if_box=True)

    # =========== 数据验证 =============
    # 与原R流程运行的结果比较正确性:
    if int(need_test) > 0:
        logger.info('数据验证-start')

        def check_out(my_out_path, R_out_path):
            my_out = spark.read.parquet(my_out_path)
            R_out = spark.read.parquet(R_out_path)
            R_out = R_out.where(R_out.Date/100 < 2020)

            # 检查内容：列缺失，列的类型，列的值
            for colname, coltype in R_out.dtypes:
                # 列是否缺失
                if colname not in my_out.columns:
                    logger.warning ("miss columns:", colname)
                else:
                    # 数据类型检查
                    if my_out.select(colname).dtypes[0][1] != coltype:
                        logger.warning("different type columns: " + colname + ", " + my_out.select(colname).dtypes[0][1] + ", " + "right type: " + coltype)

                    # 数值列的值检查
                    if coltype == "double" or coltype == "int":
                        sum_my_out = my_out.groupBy().sum(colname).toPandas().iloc[0, 0]
                        sum_R = R_out.groupBy().sum(colname).toPandas().iloc[0, 0]
                        # logger.info(colname, sum_raw_data, sum_R)
                        if (sum_my_out - sum_R) != 0:
                            logger.warning("different value(sum) columns: " + colname + ", " + str(sum_my_out) + ", " + "right value: " + str(sum_R))


        my_out_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/京新/MAX_result/MAX_result_201801-202004他汀_hosp_level"
        R_out_path = u"/common/projects/max/京新/MAX_result/MAX_result_201801-202004他汀_hosp_level"
        logger.info(u"他汀")
        check_out(my_out_path, R_out_path)
        my_out_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/京新/MAX_result/MAX_result_201801-202004癫痫新分子_hosp_level"
        R_out_path = u"/common/projects/max/京新/MAX_result/MAX_result_201801-202004癫痫新分子_hosp_level"
        logger.info(u"癫痫新分子")
        check_out(my_out_path, R_out_path)

        logger.info('数据验证-Finish')
