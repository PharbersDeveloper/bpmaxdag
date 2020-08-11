# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as func
from phlogs.phlogs import phlogger
import os

import numpy as np
import pandas as pd
from copy import deepcopy
from pyspark.sql.types import StringType,DoubleType
from pyspark.sql.functions import pandas_udf, PandasUDFType

def execute(max_path, project_name, out_path, out_dir, doi, product_input):
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("data from s3") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instance", "1") \
        .config("spark.executor.memory", "1g") \
        .config('spark.sql.codegen.wholeStage', False) \
        .config("spark.sql.execution.arrow.enabled", "true") \
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
        
    out_path_dir = out_path + "/" + project_name + '/' + out_dir + '/' + doi
        
    # 输入
    df_result_tmp_path = out_path_dir + "/df_result_tmp"
    df_pnl_path = out_path_dir + "/df_pnl"
    df_pnl_mkt_path = out_path_dir + "/df_pnl_mkt"
    df_ims_share_res_path = out_path_dir + "/df_ims_share_res"
    product_input = product_input.replace(" ","").split(',')


    # 输出
    df_result_path = out_path_dir + "/df_result"
    df_factor_result_path = out_path_dir + "/df_factor_result"
    df_rlt_brf_path = out_path_dir + "/df_rlt_brf"
    
    
    # ============== 函数定义 ================
    
    def cvxpy_func_pre(rltsc, product_input):
        # rltsc is a pandas.DataFrame
      
        import numpy as np
        import cvxpy as cp
        #from cvxpy import Variable, Problem, Minimize, maximum, abs, ECOS
        
        fst_prd = 3
        bias = 2
        
        rltsc = rltsc.fillna(0)
        #print (len(rltsc))
        f = cp.Variable()
        poi_ratio = {}
        mkt_ratio = {}
    
        for iprd in range(len(rltsc.index)):
            if rltsc["ims_poi_vol"][iprd] == 0:
                poi_ratio[iprd] = 0
            else:
                poi_ratio[iprd] = np.divide(
                    (rltsc["poi_vol"][iprd] - rltsc["sales_pnl"][iprd]) * f + rltsc["sales_pnl"][iprd],
                    rltsc["ims_poi_vol"][iprd]) - 1
            if rltsc["ims_mkt_vol"][iprd] == 0:
                mkt_ratio[iprd] = 0
            else:
                mkt_ratio[iprd] = np.divide(
                    (rltsc["mkt_vol"][iprd] - rltsc["sales_pnl_mkt"][iprd]) * f + rltsc["sales_pnl_mkt"][iprd],
                    rltsc["ims_mkt_vol"][iprd]) - 1
    
        par = []
        for s in range(len(rltsc.index)):
            if rltsc["poi"][s] in product_input[:fst_prd]:
                par += ["np.divide(cp.abs(poi_ratio[%s])," % s + str(bias) + ")"]
                par += ["cp.abs(mkt_ratio[%s])" % s]
                
        # 添加 in globals(), locals() 否则 exec 不能在有子函数的函数中
        exec ("obj=cp.Minimize(cp.maximum(" + ",".join(par) + "))") in globals(), locals()
        
        ##      obj=Minimize(max_elemwise(abs(poi_ratio[0]),abs(poi_ratio[1]),abs(poi_ratio[2]),abs(poi_ratio[3]),
        #                                abs(mkt_ratio[0]),abs(mkt_ratio[1]),abs(mkt_ratio[2]),abs(mkt_ratio[3])))
        #print(obj)
        #minimize maximum(abs((3716916.3308907785 * var0 + 13260299.0) / 18102154.0 + -1.0) / 2.0, abs((26475892.12411076 * var0 + 104242148.0) / 149269415.0 + -1.0), abs((nan * var0 + nan) / 0.0 + -1.0) / 2.0, abs((26475892.12411076 * var0 + 104242148.0) / 149269415.0 + -1.0), abs((12442094.414764605 * var0 + 49410439.0) / 74349372.0 + -1.0) / 2.0, abs((26475892.12411076 * var0 + 104242148.0) / 149269415.0 + -1.0))
        
        prob = cp.Problem(obj, [0 <= f])
        prob.solve(solver = cp.ECOS)
        #rltsc["factor"] = f.value
        #for i in range(len(rltsc)):
        #    scennew = ",".join(rltsc["scen"][i])
          
        return rltsc.assign(factor = f.value)
        
    def cvxpy_func(rltsc):
        return cvxpy_func_pre(rltsc, product_input)
    
    
    # ============== 数据执行 ================
    
    phlogger.info('数据执行-start')
    
    # 数据读取
    df_result = spark.read.parquet(df_result_tmp_path)
    df_pnl = spark.read.parquet(df_pnl_path)
    df_pnl_mkt = spark.read.parquet(df_pnl_mkt_path)
    df_ims_share_res = spark.read.parquet(df_ims_share_res_path)
    
    # df_result 处理
    df_result = df_result.join(df_pnl, on=["city", "poi"], how="left") \
        .join(df_pnl_mkt, on=["city"], how="left") \
        .join(df_ims_share_res, on=["city", "poi"], how="left")
        
    df_result = df_result.withColumn("scen", df_result["scen"].cast(StringType()))
    
    df_result = df_result.repartition(2)
    df_result.write.format("parquet") \
        .mode("overwrite").save(df_result_path)
        
    # max_outlier_factor：photfactor_udf.py
    
    schema = deepcopy(df_result.schema) # 深拷贝
    schema.add("factor", DoubleType())
    
    pudf_cvxpy_func = pandas_udf(cvxpy_func, schema, PandasUDFType.GROUPED_MAP)
    #print(pudf_cvxpy_func)
    df_factor_result=df_result.groupby(["city", "scen_id"]).apply(pudf_cvxpy_func)
    
    
    df_factor_result = df_factor_result.withColumn("poi_tmp",
                                                   ((df_factor_result.poi_vol - df_factor_result.sales_pnl) *
                                                    df_factor_result.factor + df_factor_result.sales_pnl)) \
        .withColumn("mkt_tmp",
                    ((df_factor_result.mkt_vol - df_factor_result.sales_pnl_mkt) *
                     df_factor_result.factor + df_factor_result.sales_pnl_mkt))

    df_factor_result = df_factor_result \
        .withColumn("poi_ratio", df_factor_result.poi_tmp / df_factor_result.ims_poi_vol - 1) \
        .withColumn("mkt_ratio", df_factor_result.mkt_tmp / df_factor_result.ims_mkt_vol - 1) \
        .withColumn("share_factorized", df_factor_result.poi_tmp / df_factor_result.mkt_tmp) \
        .withColumn("share_gap", (df_factor_result.poi_tmp / df_factor_result.mkt_tmp) - df_factor_result.ims_share)

    # df_factor_result.show()

    df_factor_result = df_factor_result.withColumn("rel_gap", df_factor_result.share_gap / df_factor_result.ims_share)
    # brf 的那个行转列我就不写了 @luke
    df_rlt_brf = df_factor_result.select("city", "ims_mkt_vol", "scen", "scen_id", "num_ot", "mkt_ratio", "rel_gap",
                                         "poi")
                                         
    
    # 输出结果
    df_factor_result = df_factor_result.repartition(2)
    df_factor_result.write.format("parquet") \
        .mode("overwrite").save(df_factor_result_path)
    
    phlogger.info("输出 df_factor_result 结果：".decode("utf-8") + df_factor_result_path)
        
    df_rlt_brf = df_rlt_brf.repartition(2)
    df_rlt_brf.write.format("parquet") \
        .mode("overwrite").save(df_rlt_brf_path)
        
    phlogger.info("输出 df_rlt_brf 结果：".decode("utf-8") + df_rlt_brf_path)
    
    phlogger.info('数据执行-Finish')
    
    # =========== 数据验证 =============
    if project_name == "Test":
        phlogger.info('数据验证-start')

        def check_out(my_out_path, R_out_path):
            my_out = spark.read.parquet(my_out_path)
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
                        sum_my_out = my_out.groupBy().sum(colname).toPandas().iloc[0, 0].round(2)
                        sum_R = R_out.groupBy().sum(colname).toPandas().iloc[0, 0].round(2)
                        # phlogger.info(colname, sum_raw_data, sum_R)
                        if (sum_my_out - sum_R) != 0:
                            phlogger.warning("different value(sum) columns: " + colname + ", " + str(sum_my_out) + ", " + "right value: " + str(sum_R))
        
        my_out_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Test/outlier/AZ16/df_result"
        R_out_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Test/outlier/AZ16df_result"
        print(u"df_result")
        check_out(my_out_path, R_out_path)

        my_out_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Test/outlier/AZ16/df_factor_result"
        R_out_path = "s3a://ph-max-auto/v0.0.1-2020-06-08/Test/outlier/AZ16_df_factor_result"
        print(u"df_factor")
        check_out(my_out_path, R_out_path)

        phlogger.info('数据验证-Finish')
                                 
    return [df_factor_result, df_rlt_brf]    

