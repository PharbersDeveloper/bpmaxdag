# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phlogs.phlogs import phlogger
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DoubleType
from pyspark.sql import functions as func
import os

def execute(max_path, project_name, if_base, time_left, time_right, left_models, time_left_models, rest_models, time_rest_models,
all_models, other_models, out_path, need_test):
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

    phlogger.info('job5_max')

    # 输入输出
    if if_base == "F":
        if_base = False
    elif if_base == "T":
        if_base = True
    if left_models:
        left_models = left_models.replace(", ",",").split(",")
    else:
        left_models = []
    if rest_models:
        rest_models = rest_models.replace(", ",",").split(",")
    else:
        rest_models = []
    time_parameters = [int(time_left), int(time_right), left_models, int(time_left_models), rest_models, int(time_rest_models)]
    if all_models:
        all_models = all_models.replace(", ",",").split(",")
    if other_models:
        other_models = other_models.replace(", ",",").split(",")

    if project_name == "Sanofi" or project_name == "AZ":
        project_path = max_path + "/AZ_Sanofi/"
    else:
        project_path = max_path + "/" + project_name

    out_path = out_path + '/' + project_name

    # 计算max 函数
    def calculate_max(market, if_base=False, if_box=False):

        phlogger.info('market:' + market)

        # =========== 输入 =============
        # 根据 market 选择 universe 文件：choose_uni
        if project_name == "Sanofi" or project_name == "AZ":
            if market in ['SNY6', 'SNY10', 'SNY12', 'SNY13', 'AZ12', 'AZ18', 'AZ21']:
                universe_path = project_path + '/universe_az_sanofi_onc'
            elif market in ['SNY5', 'SNY9', 'AZ10', 'AZ11', 'AZ15', 'AZ16', 'AZ14', 'AZ26', 'AZ24']:
                universe_path = project_path + '/universe_az_sanofi_mch'
            else:
                universe_path = project_path + '/universe_az_sanofi_base'
        elif project_name == "Astellas":
            if market in ["卫喜康市场".decode("utf-8")]:
                universe_path = project_path + u'/universe_卫喜康市场'
            else:
                universe_path = project_path + u'/universe_其他'
        elif project_name == "京新".decode("utf-8"):
            if market in ["他汀".decode("utf-8")]:
                universe_path = project_path + u'/universe_他汀'
            else:
                universe_path = project_path + '/universe_base'
            
        else:
            universe_path = project_path + '/universe_base'

        # universe_outlier_path 以及 factor_path 文件选择
        universe_outlier_path = project_path + "/universe/universe_ot_" + market
        if if_base:
            factor_path = project_path + "/factor/factor_base"
        else:
            factor_path = project_path + "/factor/factor_" + market

        # panel 文件选择与读取 获得 original_panel
        if project_name == "Sanofi":
            panel_box_path = project_path + "/panel_box-result_Sanofi"
            panel_path = project_path + "/panel-result_Sanofi"
        elif project_name == "AZ":
            panel_box_path = project_path + "/panel_box-result_AZ"
            panel_path = max_path + "/AZ/" + "/panel_result"
        else:
            panel_box_path = project_path + "/panel_box-result"
            panel_path = project_path + "/panel_result"

        if if_box:
            original_panel_path = panel_box_path
        else:
            original_panel_path = panel_path


        # =========== 数据检查 =============
        phlogger.info('数据检查-start')

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
        if ("Hosp_name" not in colnames_universe) and ("HOSP_NAME" not in colnames_universe):
            misscols_dict["universe"].append("Hosp_name/HOSP_NAME")
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
        if ("Hosp_name" not in colnames_universe) and ("HOSP_NAME" not in colnames_universe):
            misscols_dict["universe_outlier"].append("Hosp_name/HOSP_NAME")
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
            phlogger.error('miss columns: %s' % (misscols_dict_final))
            raise ValueError('miss columns: %s' % (misscols_dict_final))

        phlogger.info('数据检查-Pass')

        # =========== 数据执行 =============

        phlogger.info('数据执行-start')

        # 选择 market 的时间范围：choose_months
        time_left = time_parameters[0]
        time_right = time_parameters[1]
        left_models = time_parameters[2]
        time_left_models = time_parameters[3]
        rest_models = time_parameters[4]
        time_rest_models = time_parameters[5]

        if market in left_models:
            time_left = time_left_models
        if market in rest_models:
            time_right = time_rest_models
        time_range = str(time_left) + '-' + str(time_right)

        # universe_outlier 文件读取与处理：read_uni_ot
        # universe_outlier = spark.read.parquet(universe_outlier_path)
        for col in universe_outlier.columns:
            if col in ["City_Tier", "CITYGROUP"]:
                universe_outlier = universe_outlier.withColumnRenamed(col, "City_Tier_2010")
        universe_outlier = universe_outlier.withColumnRenamed("Panel_ID", "PHA") \
            .withColumnRenamed("Hosp_name", "HOSP_NAME")
        universe_outlier = universe_outlier.withColumn("City_Tier_2010", universe_outlier["City_Tier_2010"].cast(StringType()))
        universe_outlier = universe_outlier.select("PHA", "Est_DrugIncome_RMB", "PANEL", "Seg", "BEDSIZE")

        # universe 文件读取与处理：read_universe
        # universe = spark.read.parquet(universe_path)
        for col in universe.columns:
            if col in ["City_Tier", "CITYGROUP"]:
                universe = universe.withColumnRenamed(col, "City_Tier_2010")
        universe = universe.withColumnRenamed("Panel_ID", "PHA") \
            .withColumnRenamed("Hosp_name", "HOSP_NAME")
        universe = universe.withColumn("City_Tier_2010", universe["City_Tier_2010"].cast(StringType()))

        # panel 文件读取 获得 original_panel
        # original_panel = spark.read.parquet(original_panel_path)
        original_panel = original_panel.where((original_panel.DOI == market) & (original_panel.Date >= time_left) & (original_panel.Date <= time_right))


        # 获得 panel, panel_seg：group_panel_by_seg

        # panel：整理成max的格式，包含了所有在universe的panel列标记为1的医院，当作所有样本医院的max
        universe_panel_all = universe.where(universe.PANEL == 1).select('PHA', 'BEDSIZE', 'PANEL', 'Seg')
        panel = original_panel \
            .join(universe_panel_all, original_panel.HOSP_ID == universe_panel_all.PHA, how="inner") \
            .groupBy('PHA', 'Province', 'City', 'Date', 'Molecule', 'Prod_Name', 'BEDSIZE', 'PANEL', 'Seg') \
            .agg(func.sum("Sales").alias("Predict_Sales"), func.sum("Units").alias("Predict_Unit"))

        # panel_seg：整理成seg层面，包含了所有在universe_ot的panel列标记为1的医院，可以用来得到非样本医院的max
        panel_drugincome = universe_outlier.where(universe_outlier.PANEL == 1) \
            .groupBy("Seg") \
            .agg(func.sum("Est_DrugIncome_RMB").alias("DrugIncome_Panel"))
        original_panel_tmp = original_panel.join(universe_outlier, original_panel.HOSP_ID == universe_outlier.PHA, how='left')
        panel_seg = original_panel_tmp.where(original_panel_tmp.PANEL == 1) \
            .groupBy('Date', 'Prod_Name', 'Seg', 'Molecule') \
            .agg(func.sum("Sales").alias("Sales_Panel"), func.sum("Units").alias("Units_Panel"))
        panel_seg = panel_seg.join(panel_drugincome, on="Seg", how="left")

        # 将非样本的segment和factor等信息合并起来：get_uni_with_factor
        # factor = spark.read.parquet(factor_path)
        if "factor" not in factor.columns:
            factor = factor.withColumnRenamed("factor_new", "factor")
        factor = factor.select('City', 'factor')
        universe_factor_panel = universe.join(factor, on="City", how="left")
        universe_factor_panel = universe_factor_panel \
            .withColumn("factor", func.when(func.isnull(universe_factor_panel.factor), func.lit(1)).otherwise(universe_factor_panel.factor)) \
            .where(universe_factor_panel.PANEL == 0) \
            .select('Province', 'City', 'PHA', 'Est_DrugIncome_RMB', 'Seg', 'BEDSIZE', 'PANEL', 'factor')

        # 为这些非样本医院匹配上样本金额、产品、年月、所在segment的drugincome之和
        max_result = universe_factor_panel.join(panel_seg, on="Seg", how="left")

        # 预测值等于样本金额乘上当前医院drugincome再除以所在segment的drugincome之和
        max_result = max_result.withColumn("Predict_Sales", (max_result.Sales_Panel / max_result.DrugIncome_Panel) * max_result.Est_DrugIncome_RMB) \
            .withColumn("Predict_Unit", (max_result.Units_Panel / max_result.DrugIncome_Panel) * max_result.Est_DrugIncome_RMB)

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
            max_path = out_path + "/MAX_result/MAX_result_" + time_range + market + "_hosp_level_box"
            max_result.write.format("parquet") \
                .mode("overwrite").save(max_path)
        else:
            max_path = out_path + "/MAX_result/MAX_result_" + time_range + market + "_hosp_level"
            max_result.write.format("parquet") \
                .mode("overwrite").save(max_path)

        # 输出excel
        '''
        max_excel = max_result.where(max_result.BEDSIZE > 99) \
            .groupBy('Province', 'City', 'PANEL', "Prod_Name", 'Date') \
            .agg(func.sum("Predict_Sales").alias("Predict_Sales"), func.sum("Predict_Unit").alias("Predict_Unit"))
        max_excel = max_excel.toPandas()
        # print(np.sort(max_c["Date"].unique(), axis=None))
    
        if if_box:
            # max_c_path = project_path_local + '/MODEL/' + mkt + '_MAX_result_100bed_' + time_range + '_box' + time.strftime("%Y-%m-%d", time.localtime()) + '.xlsx'
            max_excel_path = project_path_local_c9 + '/MODEL/' + market + '_MAX_result_100bed_' + time_range + '_box' + '.xlsx'
            max_excel.to_excel(max_excel_path, index=False)
            # 传输到s3
            s3.put_object("ph-max-auto", max_path_local.split("/")[-1] + "/" + project_name + "/" + '/'.join(max_excel_path.split("/")[-2:]), max_excel_path)
        else:
            # max_c_path = project_path_local + '/MODEL/' + mkt + '_MAX_result_100bed_' + time_range + time.strftime("%Y-%m-%d", time.localtime()) + '.xlsx'
            max_excel_path = project_path_local_c9 + '/MODEL/' + market + '_MAX_result_100bed_' + time_range + '.xlsx'
            max_excel.to_excel(max_excel_path, index=False)
            # 传输到s3
            s3.put_object("ph-max-auto", max_path_local.split("/")[-1] + "/" + project_name + "/" + '/'.join(max_excel_path.split("/")[-2:]), max_excel_path)
        '''

        phlogger.info('数据执行-Finish')


    # 执行函数
    if all_models:
        for i in all_models:
            # i = i.decode("gb18030")
            calculate_max(i, if_base=if_base, if_box=False)

    if other_models:
        for i in other_models:
            calculate_max(i, if_base=if_base, if_box=True)

    # =========== 数据验证 =============
    # 与原R流程运行的结果比较正确性:
    if int(need_test) > 0:
        phlogger.info('数据验证-start')

        def check_out(my_out_path, R_out_path):
            my_out = spark.read.parquet(my_out_path)
            R_out = spark.read.parquet(R_out_path)
            R_out = R_out.where(R_out.Date/100 < 2020)

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


        my_out_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/京新/MAX_result/MAX_result_201801-202004他汀_hosp_level"
        R_out_path = u"/common/projects/max/京新/MAX_result/MAX_result_201801-202004他汀_hosp_level"
        phlogger.info(u"他汀")
        check_out(my_out_path, R_out_path)
        my_out_path = u"s3a://ph-max-auto/v0.0.1-2020-06-08/京新/MAX_result/MAX_result_201801-202004癫痫新分子_hosp_level"
        R_out_path = u"/common/projects/max/京新/MAX_result/MAX_result_201801-202004癫痫新分子_hosp_level"
        phlogger.info(u"癫痫新分子")
        check_out(my_out_path, R_out_path)

        phlogger.info('数据验证-Finish')
