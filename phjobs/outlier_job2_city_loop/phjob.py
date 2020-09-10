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

from pyspark.sql.functions import udf
import numpy as np
import pandas as pd
import itertools

def execute(max_path, project_name, out_path, out_dir, doi, product_input, cities, num_ot_max, sample_max):
    os.environ["PYSPARK_PYTHON"] = "python2"    
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
    out_path_dir = out_path + "/" + project_name + '/' + out_dir + '/' + doi
    df_EIA_res_path = out_path_dir + "/df_EIA_res"
    df_seg_city_path = out_path_dir + "/df_seg_city"
    cities = cities.replace(" ","").split(',')
    product_input = product_input.replace(" ","").split(',')
    
    '''
    @num_ot_max: 为每个城市选择outlier的数量上限
    @sample_max: 选outlier的范围，该城市最大的sample_max家医院
    '''
    num_ot_max = int(num_ot_max)
    sample_max = int(sample_max)
    
    # 输出
    tmp_df_result_path = out_path_dir + "/df_result_tmp"
    
    # 根据 product_input 构造sql_content 语句
    product_for_sql = [i for i in product_input for n in range(2)]
    product_for_sql = str(product_for_sql).replace("[","").replace("]","").replace("u'","'")
    
    new_product_for_sql = ""
    for n, i in enumerate(product_for_sql.split(",")):
        if n == 0:
            new_product_for_sql += i
        elif (n % 2) == 0:
            new_product_for_sql += ',' + i
        else:
            i = i.replace("'","`")
            new_product_for_sql += ',' + i
    
    sql_content = "select `mkt_vol`, `scen_id`, `scen`, `city`, `num_ot`, `vol_ot`, stack("  + \
                str(len(product_input)) + ',' + \
                new_product_for_sql + \
                ") as (`poi`, `poi_vol` ) from  v_pivot"

    sql_content = eval('u"%s"' % sql_content)

    #sql_content = '''select `mkt_vol`, `scen_id`, `scen`, `city`, `num_ot`, `vol_ot`,
    #             stack(3, '普米克令舒', `普米克令舒`, 'Others-Pulmicort', `Others-Pulmicort`, '益索', `益索`) as (`poi`, `poi_vol` )
    #             from  v_pivot
    #          '''

    
    # ==============  函数定义 ================
    
    # 对 product_input每列 + other列 累加求和，生成mkt_size列
    def cal_mkt(list_prod, df_eia):
        df_eia = df_eia.withColumn("mkt_size", func.lit(0))
        for p in list_prod:
            df_eia = df_eia.withColumn("mkt_size",
                                       df_eia[p] + df_eia["mkt_size"])
        df_eia = df_eia.withColumn("mkt_size",
                                   df_eia["other"] + df_eia["mkt_size"])
        return df_eia
        
    def udf_add_struct(prd_prod):
        schema = StructType(
            []
        )
        for i in prd_prod:
            schema.add(
                    StructField(i, DoubleType(), True)
                )
        return schema
        
    def udf_rename(df_eia, list_prod, suffix=""):
        for p in list_prod:
            df_eia = df_eia.withColumnRenamed("sum("+p+")",
                                              p+suffix)
        return df_eia
        
    def udf_new_columns(df_oth_seg, list_prod, suffix = "", value = 0):
        for p in list_prod:
            df_oth_seg = df_oth_seg.withColumn(p+suffix, func.lit(value))
        return df_oth_seg
        
    def sum_columns(df_eia, existing_names, result_name, new_name = True):
        if(new_name):
            df_eia = df_eia.withColumn(result_name, func.lit(0))
        for p in existing_names:
            df_eia = df_eia.withColumn(result_name,
                                       df_eia[p.encode("utf-8")] + df_eia[result_name])
    
        return df_eia
        
    @udf(ArrayType(IntegerType()))
    def rep_EIA():
        res = []
        for x in range(256):
            res.append(x)
        return res
    
    
    @udf(BooleanType())
    def is_scen(lst, cur):
        if isinstance(lst, list):
            if cur in lst:
                return True
            else:
                return False
        else:
            return False
        
    def max_outlier_seg_wo_ot_spark(spark, df_EIA_res_iter, ct, seg_wo_ot, ):
        # product_input = ["加罗宁", "凯纷", "诺扬"]
        # schema = StructType([
        #     StructField("加罗宁_fd", DoubleType(), True),
        #     StructField("凯纷_fd", DoubleType(), True),
        #     StructField("诺扬_fd", DoubleType(), True),
        #     StructField("other_fd", DoubleType(), True)])
        schema = udf_add_struct([p+"_fd" for p in product_input] + ["other_fd"])
        df_result = spark.createDataFrame([], schema)
        for z in seg_wo_ot:
            df_other_seg = df_EIA_res_iter.where(df_EIA_res_iter.Seg == z)
    
            df_oth_seg = df_other_seg.groupBy("Date").sum(*product_input+["other", "Est_DrugIncome_RMB"])
    
            df_oth_seg = udf_rename(df_oth_seg, product_input+["other", "Est_DrugIncome_RMB"])
    
            df_oth_seg_p0_bed100 = df_other_seg.where(
                (df_other_seg.BEDSIZE >= 100) &
                (df_other_seg.City == ct) &
                (df_other_seg.PANEL == 0)
            ).groupBy("Date").sum(*product_input+["other", "Est_DrugIncome_RMB"])
    
            df_oth_seg_p0_bed100 = udf_rename(df_oth_seg_p0_bed100, \
                                              product_input+["other", "Est_DrugIncome_RMB"], "_p0_bed100")
    
            df_oth_seg_p1_bed100 = df_other_seg.where(
                (df_other_seg.BEDSIZE >= 100) &
                (df_other_seg.City == ct) &
                (df_other_seg.PANEL == 1)
            ).groupBy("Date").sum(*product_input+["other", "Est_DrugIncome_RMB"])
    
            df_oth_seg_p1_bed100 = udf_rename(df_oth_seg_p1_bed100, \
                                              product_input+["other", "Est_DrugIncome_RMB"], "_p1_bed100")
    
            df_oth_seg_p1 = df_other_seg.where(
                (df_other_seg.PANEL == 1)
            ).groupBy("Date").sum(*product_input+["other", "Est_DrugIncome_RMB"])
    
            df_oth_seg_p1 = udf_rename(df_oth_seg_p1,\
                                       product_input+["other", "Est_DrugIncome_RMB"], "_p1_other")
    
    
            df_oth_seg = df_oth_seg.join(df_oth_seg_p0_bed100, on="Date", how="left")
            df_oth_seg = df_oth_seg.join(df_oth_seg_p1_bed100, on="Date", how="left")
            df_oth_seg = df_oth_seg.join(df_oth_seg_p1, on="Date", how="left").fillna(0)
    
            if df_oth_seg_p0_bed100.count() == 0:
                df_oth_seg = df_oth_seg.withColumn("w", func.lit(0))
            else:
                df_oth_seg = df_oth_seg \
                    .withColumn("w", df_oth_seg.Est_DrugIncome_RMB_p0_bed100 / df_oth_seg.Est_DrugIncome_RMB_p1_other)
    
            if df_oth_seg_p1_bed100.count() == 0:
                df_oth_seg = udf_new_columns(df_oth_seg, product_input, "_p1_bed100")
                # df_oth_seg = df_oth_seg.withColumn("加罗宁_p1_bed100", func.lit(0)) \
                #     .withColumn("凯纷_p1_bed100", func.lit(0)) \
                #     .withColumn("诺扬_p1_bed100", func.lit(0)) \
                #     .withColumn("other_p1_bed100", func.lit(0))
    
            df_oth_seg = df_oth_seg.withColumn("other_fd", df_oth_seg["other"] * df_oth_seg.w + df_oth_seg["other_p1_bed100"])
    
            for iprd in product_input:
                df_oth_seg = df_oth_seg \
                    .withColumn(iprd + "_fd", df_oth_seg[iprd] * df_oth_seg.w + df_oth_seg[iprd + "_p1_bed100"])
    
            # df_result = df_result.union(df_oth_seg.groupBy().sum("加罗宁_fd", "凯纷_fd", "诺扬_fd", "other_fd"))
            df_result = df_result.union(df_oth_seg.select(*[p+"_fd" for p in product_input]+["other_fd"]))
            # [p+"_fd" for p in product_input], "other_fd"
        sum_result = df_result.groupBy().sum(*[p+"_fd" for p in product_input]+["other_fd"]).toPandas()
        #sum_result = udf_rename(sum_result, [p+"_fd" for p in product_input]+["other_fd"]).toPandas()
        # print sum_result.columns
        # print sum_result
        #chk = sum_result.at[0, 1]
        other_seg_oth = sum_result.at[0, "sum(other_fd)"]
        other_seg_poi = {}
        for iprd in product_input:
            #print "sum(" + iprd + "_fd)"
            #other_seg_poi[iprd] = sum_result.at[0, iprd+"_fd"]
            other_seg_poi[iprd] = sum_result.at[0, ("sum(" + iprd + "_fd)").encode("utf-8")]
    
        #other_seg_oth = sum_result.at[0, "other_fd"]
        #other_seg_oth = sum_result.at[0, "sum(other_fd)"]
    
        return [other_seg_oth, other_seg_poi]
        
    def max_outlier_seg_scen_ot_spark_2(spark, df_EIA_res_cur,
                                    df_panel, ct, scen,
                                    ot_seg, other_seg_poi, other_seg_oth):
    
        #product_input = ["加罗宁", "凯纷", "诺扬"]
    
        # schema = StructType([
        #     StructField("scen", ArrayType(StringType()), True),
        # ])
        arr = np.array(scen[ot_seg])
        df = pd.DataFrame(data=arr.flatten())
        df.columns = ["scen"]
        df["scen_id"] = range(0, len(df))
        df_scen_ot_seg = spark.createDataFrame(df)
    
        df_scen_ot_seg = df_scen_ot_seg.withColumn("num_ot", func.size("scen"))
    
        # monotonically_increasing_id 有问题
        # df_scen_ot_seg = df_scen_ot_seg.repartition(1).withColumn("scen_id", func.monotonically_increasing_id()) \
        #     .withColumn("num_ot", func.size("scen"))
    
    
        df_EIA_res_cur = df_EIA_res_cur.withColumn("scen_id_lst", rep_EIA())
        df_EIA_res_cur = df_EIA_res_cur.select("*", func.explode("scen_id_lst").alias("scen_id")).drop("scen_id_lst")
    
        df_EIA_res_cur = df_EIA_res_cur.join(df_scen_ot_seg, on=["scen_id"], how="left").fillna(0)
        # df_EIA_res_cur.show(1000)
    
        df_EIA_res_cur = df_EIA_res_cur.withColumn("is_bed_gt_100", df_EIA_res_cur.BEDSIZE >= 100) \
            .withColumn("is_panel", df_EIA_res_cur.PANEL == 1) \
            .withColumn("is_city", df_EIA_res_cur.City == ct) \
            .withColumn("is_scen", is_scen(df_EIA_res_cur.scen, df_EIA_res_cur.HOSP_ID))
    
        df_EIA_res_cur.persist()
    
        # 0. vol_ot
        df_EIA_res_cur_vol_ot = df_EIA_res_cur.where(df_EIA_res_cur.is_scen) \
            .groupBy("scen_id").sum("Est_DrugIncome_RMB") \
            .withColumnRenamed("sum(Est_DrugIncome_RMB)", "vol_ot")
    
        # df_EIA_res_cur = df_EIA_res_cur.join(df_EIA_res_cur_vol_ot, on="scen_id", how="left").fillna(0)
    
        # 1. poi_ot & oth_ot
        df_EIA_res_cur_poi_ot = df_EIA_res_cur.where(
            df_EIA_res_cur.is_bed_gt_100 & df_EIA_res_cur.is_city & df_EIA_res_cur.is_scen)
        # df_EIA_res_cur_poi_ot = df_EIA_res_cur_poi_ot.groupBy("scen_id").sum(*[p for p in product_input]+["other"]) \
        #     .withColumnRenamed("sum(加罗宁)", "加罗宁_poi_ot") \
        #     .withColumnRenamed("sum(凯纷)", "凯纷_poi_ot") \
        #     .withColumnRenamed("sum(诺扬)", "诺扬_poi_ot") \
        #     .withColumnRenamed("sum(其它)", "oth_ot").fillna(0.0)
        df_EIA_res_cur_poi_ot = df_EIA_res_cur_poi_ot.groupBy("scen_id").sum(*[p for p in product_input]+["other"])
        df_EIA_res_cur_poi_ot = udf_rename(df_EIA_res_cur_poi_ot, [p for p in product_input]+["other"], "_poi_ot")
        df_EIA_res_cur_poi_ot = df_EIA_res_cur_poi_ot.fillna(0.0)
    
        # 1.1  数据还需要补全
        df_EIA_res_cur_poi_ot = df_scen_ot_seg.join(df_EIA_res_cur_poi_ot, on="scen_id", how="left").fillna(0)
    
        # 2. rest
        df_EIA_res_rest = df_EIA_res_cur.where(~df_EIA_res_cur.is_scen)
        # df_rest_seg = df_EIA_res_rest.groupBy("scen_id", "Date").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
        #     .withColumnRenamed("sum(加罗宁)", "加罗宁") \
        #     .withColumnRenamed("sum(凯纷)", "凯纷") \
        #     .withColumnRenamed("sum(诺扬)", "诺扬") \
        #     .withColumnRenamed("sum(其它)", "其它") \
        #     .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB").fillna(0.0)
    
        df_rest_seg = df_EIA_res_rest.groupBy("scen_id", "Date").sum(*[p for p in product_input]+["other","Est_DrugIncome_RMB"])
        df_rest_seg = udf_rename(df_rest_seg, [p for p in product_input]+["other","Est_DrugIncome_RMB"])
        df_rest_seg = df_rest_seg.fillna(0.0)
    
        df_rest_seg_p0_bed100 = df_EIA_res_rest.where(
            (df_EIA_res_rest.BEDSIZE >= 100) &
            (df_EIA_res_rest.City == ct) &
            (df_EIA_res_rest.PANEL == 0)
        )#.groupBy("scen_id", "Date").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
            # .withColumnRenamed("sum(加罗宁)", "加罗宁_p0_bed100") \
            # .withColumnRenamed("sum(凯纷)", "凯纷_p0_bed100") \
            # .withColumnRenamed("sum(诺扬)", "诺扬_p0_bed100") \
            # .withColumnRenamed("sum(其它)", "其它_p0_bed100") \
            # .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB_p0_bed100").fillna(0.0)
    
        df_rest_seg_p0_bed100 = df_rest_seg_p0_bed100.groupBy("scen_id", "Date").sum(*[p for p in product_input]+["other","Est_DrugIncome_RMB"])
        df_rest_seg_p0_bed100 = udf_rename(df_rest_seg_p0_bed100, [p for p in product_input]+["other","Est_DrugIncome_RMB"],"_p0_bed100")
        df_rest_seg_p0_bed100 = df_rest_seg_p0_bed100.fillna(0.0)
    
        df_rest_seg_p1_bed100 = df_EIA_res_rest.where(
            (df_EIA_res_rest.BEDSIZE >= 100) &
            (df_EIA_res_rest.City == ct) &
            (df_EIA_res_rest.PANEL == 1)
        )#.groupBy("scen_id", "Date").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
            # .withColumnRenamed("sum(加罗宁)", "加罗宁_p1_bed100") \
            # .withColumnRenamed("sum(凯纷)", "凯纷_p1_bed100") \
            # .withColumnRenamed("sum(诺扬)", "诺扬_p1_bed100") \
            # .withColumnRenamed("sum(其它)", "其它_p1_bed100") \
            # .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB_p1_bed100").fillna(0.0)
    
        df_rest_seg_p1_bed100 = df_rest_seg_p1_bed100.groupBy("scen_id", "Date").sum(*[p for p in product_input]+["other","Est_DrugIncome_RMB"])
        df_rest_seg_p1_bed100 = udf_rename(df_rest_seg_p1_bed100, [p for p in product_input]+["other","Est_DrugIncome_RMB"],"_p1_bed100")
        df_rest_seg_p1_bed100 = df_rest_seg_p1_bed100.fillna(0.0)
    
        df_rest_seg_p1 = df_EIA_res_rest.where(
            (df_EIA_res_rest.PANEL == 1)
        )#.groupBy("scen_id", "Date").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
            # .withColumnRenamed("sum(加罗宁)", "加罗宁_p1_other") \
            # .withColumnRenamed("sum(凯纷)", "凯纷_p1_other") \
            # .withColumnRenamed("sum(诺扬)", "诺扬_p1_other") \
            # .withColumnRenamed("sum(其它)", "其它_p1_other") \
            # .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB_p1_other").fillna(0.0)
    
        df_rest_seg_p1 = df_rest_seg_p1.groupBy("scen_id", "Date").sum(*[p for p in product_input]+["other","Est_DrugIncome_RMB"])
        df_rest_seg_p1 = udf_rename(df_rest_seg_p1, [p for p in product_input]+["other","Est_DrugIncome_RMB"],"_p1_other")
        df_rest_seg_p1 = df_rest_seg_p1.fillna(0.0)
    
        df_rest_seg = df_rest_seg.join(df_rest_seg_p0_bed100, on=["scen_id", "Date"], how="left")
        df_rest_seg = df_rest_seg.join(df_rest_seg_p1_bed100, on=["scen_id", "Date"], how="left")
        df_rest_seg = df_rest_seg.join(df_rest_seg_p1, on=["scen_id", "Date"], how="left").fillna(0.0)
    
        df_rest_seg_p0_bed100_scen_count = df_rest_seg_p0_bed100.withColumn("count", func.lit(1))
        df_rest_seg_p0_bed100_scen_count = df_rest_seg_p0_bed100_scen_count.groupBy("scen_id") \
            .sum("count").withColumnRenamed("sum(count)", "count_p0_bed100")
        df_rest_seg = df_rest_seg.join(df_rest_seg_p0_bed100_scen_count, on="scen_id", how="left")
        df_rest_seg = df_rest_seg.withColumn("w",
                                             func.when(df_rest_seg.count_p0_bed100 == 0, func.lit(0))
                                             .otherwise(
                                                 df_rest_seg.Est_DrugIncome_RMB_p0_bed100 / df_rest_seg.Est_DrugIncome_RMB_p1_other))
    
        for iprd in product_input:
            df_rest_seg = df_rest_seg \
                .withColumn(iprd + "_fd",
                            df_rest_seg[iprd] * df_rest_seg.w + df_rest_seg[iprd + "_p1_bed100"])
    
        df_rest_seg = df_rest_seg \
            .withColumn("other_fd",
                        df_rest_seg["other"] * df_rest_seg.w + df_rest_seg["other_p1_bed100"])
    
        # df_rest_poi_oth = df_rest_seg.groupBy("scen_id").sum("加罗宁_fd", "凯纷_fd", "诺扬_fd", "oth_fd") \
        #     .withColumnRenamed("sum(加罗宁_fd)", "加罗宁_rest_poi") \
        #     .withColumnRenamed("sum(凯纷_fd)", "凯纷_rest_poi") \
        #     .withColumnRenamed("sum(诺扬_fd)", "诺扬_rest_poi") \
        #     .withColumnRenamed("sum(oth_fd)", "oth_rest_oth")
    
        df_rest_poi_oth = df_rest_seg.groupBy("scen_id").sum(*[p+"_fd" for p in product_input]+["other_fd"])
        df_rest_poi_oth = udf_rename(df_rest_poi_oth, [p+"_fd" for p in product_input]+["other_fd"],"_rest_poi")
    
    
        #df_EIA_res_cur_poi_ot.show()
        df_result = df_EIA_res_cur_poi_ot.join(df_rest_poi_oth, on="scen_id", how="left")
        df_result = df_result.join(df_EIA_res_cur_vol_ot, on="scen_id", how="left")
        # df_result.show()
        #None
        other_seg_poi_sum = sum(filter(None, other_seg_poi.values()))
    
        # df_result = df_result.withColumn("mkt_vol",
        #                                  df_result["加罗宁_rest_poi"] + df_result["凯纷_rest_poi"] +
        #                                  df_result["诺扬_rest_poi"] + df_result["oth_rest_oth"] +
        #                                  df_result["加罗宁_poi_ot"] + df_result["凯纷_poi_ot"] +
        #                                  df_result["诺扬_poi_ot"] + df_result["oth_ot"] + func.lit(other_seg_oth) +
        #                                  func.lit(other_seg_poi_sum))
    
        df_result = sum_columns(df_result, [p+"_fd_rest_poi" for p in product_input+["other"]]+ \
                                [p+"_poi_ot" for p in product_input+["other"]], "mkt_vol")
        df_result = df_result.withColumn("mkt_vol",
                                         df_result["mkt_vol"]+func.lit(other_seg_oth) +
                                         func.lit(other_seg_poi_sum))
    
        for p in product_input:
            df_result = sum_columns(df_result, [p+"_fd_rest_poi",p+"_poi_ot"], p)
            df_result = df_result.withColumn(p,
                                             df_result[p]+func.lit(other_seg_poi[p]))
    
        # df_result = df_result.withColumn("加罗宁",
        #                                  df_result["加罗宁_rest_poi"] +
        #                                  df_result["加罗宁_poi_ot"] + func.lit(other_seg_poi["加罗宁"]))
        # df_result = df_result.withColumn("凯纷",
        #                                  df_result["凯纷_rest_poi"] +
        #                                  df_result["凯纷_poi_ot"] + func.lit(other_seg_poi["凯纷"]))
        # df_result = df_result.withColumn("诺扬",
        #                                  df_result["诺扬_rest_poi"] +
        #                                  df_result["诺扬_poi_ot"] + func.lit(other_seg_poi["诺扬"]))
    
        df_result = df_result.withColumn("city", func.lit(ct))
        df_result = df_result.select(*["scen_id", "num_ot", "vol_ot", "scen", "city"]+ product_input+ ["mkt_vol"])
    
        df_result.createOrReplaceTempView('v_pivot')
        # sql_content = '''select `mkt_vol`, `scen_id`, `scen`, `city`, `num_ot`, `vol_ot`,
        #                  stack(3, '加罗宁', `加罗宁`, '凯纷', `凯纷`, '诺扬', `诺扬`) as (`poi`, `poi_vol` )
        #                  from  v_pivot
        #               '''
    
        df_result = spark.sql(sql_content)
        df_result = df_result.withColumn("share", df_result.poi_vol / df_result.mkt_vol) \
            .select("poi", "scen_id", "share", "num_ot", "vol_ot", "poi_vol", "mkt_vol", "scen", "city")
        #df_result.show()
    
        return df_result
    
    # ==============  数据执行 ================
    
    # 数据读取
    df_EIA_res = spark.read.parquet(df_EIA_res_path)
    df_seg_city = spark.read.parquet(df_seg_city_path)
    
    # max_outlier_city_loop_template
    
    index = 0
    for ct in cities:
        phlogger.info(ct)

        # 通过Seg来过滤数据
        df_seg_city_iter = df_seg_city.where(df_seg_city.City == ct).select("Seg").distinct()
        df_EIA_res_iter = df_EIA_res.join(df_seg_city_iter, on=["Seg"], how="inner")
        # 对 product_input每列 + other列 累加求和，生成mkt_size列
        df_EIA_res_iter = cal_mkt(product_input, df_EIA_res_iter)

        # 策略 1: 选择最大的Seg
        # ot_seg：Est_DrugIncome_RMB最大的Seg编号
        ot_seg = df_EIA_res_iter.where(df_EIA_res_iter.PANEL == 1) \
            .groupBy("Seg").sum("Est_DrugIncome_RMB") \
            .orderBy(func.desc("sum(Est_DrugIncome_RMB)")).toPandas()["Seg"].to_numpy()[0]
        
        df_ot_city = df_EIA_res_iter.where(df_EIA_res_iter.PANEL == 1).select("Seg", "HOSP_ID").distinct()

        # 这部分计算量很小，在Driver机器单线程计算
        # cd_arr={Seg:[多个HOSP_ID]} 存储每个Seg的医院信息
        # scen={Seg:[]}
        ot_city = df_ot_city.toPandas().values
        cd_arr = {}
        scen = {}
        for i in range(len(ot_city)):
            if ot_city[i][0] in cd_arr:
                cd_arr[ot_city[i][0]] = cd_arr[ot_city[i][0]] + [ot_city[i][1]]
            else:
                cd_arr[ot_city[i][0]] = [ot_city[i][1]]

            scen[ot_city[i][0]] = []

        # 当医院的最大数量大于规定数量，取销量最多sample_max个的医院
        if len(cd_arr[ot_seg]) > sample_max:
            # smpl=np.random.choice(cd_arr[ot_seg], sample_max,p=DrugIncome_std, replace=False).tolist()
            # print ct
            if ct in [u"珠三角市", u"福厦泉市",u'金台嘉绍']:
                # print u"珠福特殊条件"
                df_tmp = df_EIA_res_iter.where(
                    (df_EIA_res_iter.HOSP_ID.isin(cd_arr[ot_seg])) &
                    (df_EIA_res_iter.Date == 201901) &
                    (df_EIA_res_iter.City == ct)
                )
            else:
                df_tmp = df_EIA_res_iter.where(
                    (df_EIA_res_iter.HOSP_ID.isin(cd_arr[ot_seg])) &
                    (df_EIA_res_iter.Date == 201901)
                )

            smpl = df_tmp.orderBy(df_tmp.Est_DrugIncome_RMB.desc()) \
                .limit(sample_max).toPandas()["HOSP_ID"].to_numpy()
        else:
            smpl = cd_arr[ot_seg]

        iter_scen = min(num_ot_max + 1, len(cd_arr[ot_seg]))

        for L in range(iter_scen):
            for subset in itertools.combinations(smpl, L):
                scen[ot_seg].append(list(subset))

        df_panel = df_EIA_res_iter.where(df_EIA_res_iter.PANEL == 1).select("HOSP_ID").distinct()

        df_EIA_res_cur = df_EIA_res_iter.where(df_EIA_res_iter.Seg == ot_seg)

        # TODO: ot_seg 可能不存在我日
        seg_wo_ot = df_EIA_res_iter.where(df_EIA_res_iter.Seg != ot_seg) \
            .select("Seg").distinct().toPandas()["Seg"].to_numpy()

        [other_seg_oth, other_seg_poi] = max_outlier_seg_wo_ot_spark(spark, df_EIA_res_iter, ct, seg_wo_ot)

        df_result = max_outlier_seg_scen_ot_spark_2(spark, df_EIA_res_cur,
                                                  df_panel, ct, scen,
                                                  ot_seg, other_seg_poi, other_seg_oth)
        
        # TODO: 这个地方这样写可能会有大量的内存压力，
        # 因为你不知道数据是不是在计算其他数据的过程中已经被释放掉啦内存，
        # 因此会造成大量的重复计算
        # 正确的写法有两种：
        # 1. 每一个城市生成一个固定路径的中间零时文件，最后将零时文件整合成一个文件
        # 2. 利用流式数据，将所有见过append到一个文件中，两种都是可以的

        if(index == 0):
            df_result=df_result.repartition(1)
            df_result.write.format("parquet") \
                .mode("overwrite").save(tmp_df_result_path)
        else:
            df_result=df_result.repartition(1)
            df_result.write.format("parquet") \
                .mode("append").save(tmp_df_result_path)
        index = index + 1

    df_result_all = spark.read.parquet(tmp_df_result_path)
    
    return df_result_all
    
    