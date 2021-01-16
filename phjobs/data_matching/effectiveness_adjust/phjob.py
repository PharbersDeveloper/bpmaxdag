# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from ph_logs.ph_logs import phs3logger
import re
import uuid
import pkuseg
import math
import pandas as pd
import numpy as np
from math import isnan, sqrt
from pyspark.sql.types import *
from pyspark.sql.functions import when
from pyspark.sql.functions import col, upper, lit
from pyspark.sql.functions import explode, concat
from pyspark.sql.functions import regexp_replace, regexp_extract
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
from pyspark.sql.functions import array_distinct, array
from pyspark.ml.feature import StopWordsRemover
from pyspark.ml.linalg import Vectors, VectorUDT
from nltk.metrics.distance import jaro_winkler_similarity


def execute(**kwargs):
	"""
		please input your code below
		get spark session: spark = kwargs["spark"]()
	"""
	logger = phs3logger(kwargs["job_id"])
	spark = kwargs["spark"]()
	
	logger.info(kwargs)

	# input	
	depends = get_depends_path(kwargs)
	df_second_round = spark.read.parquet(depends["input"])

	dosage_mapping_path = kwargs["dosage_mapping_path"]
	df_dosage_mapping = load_dosage_mapping(spark, dosage_mapping_path)
	
	word_dict_encode_path = kwargs["word_dict_encode_path"]
	df_encode = load_word_dict_encode(spark, word_dict_encode_path)

	lexicon_path = kwargs["lexicon_path"]	
	get_seg(spark, lexicon_path)

	# output 	
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	result_path = result_path_prefix + kwargs["effective_result"]
	   
	# 6. 第二轮更改优化eff的计算方法
	df_second_round = df_second_round.withColumnRenamed("EFFTIVENESS_SPEC", "EFFTIVENESS_SPEC_FIRST")
	df_second_round = second_round_with_col_recalculate(df_second_round, df_dosage_mapping, df_encode, spark)
	# spec拆列之后的匹配算法
	df_second_round = spec_split_matching(df_second_round)
	df_second_round = df_second_round.withColumn("EFFTIVENESS_SPEC", when((df_second_round.EFFTIVENESS_SPEC_FIRST > df_second_round.EFFTIVENESS_SPEC_SPLIT), \
																		df_second_round.EFFTIVENESS_SPEC_FIRST) \
																		.otherwise(df_second_round.EFFTIVENESS_SPEC_SPLIT))
	df_second_round = df_second_round.withColumnRenamed("EFFTIVENESS_PRODUCT_NAME", "EFFTIVENESS_PRODUCT_NAME_FIRST") \
								.withColumnRenamed("EFFTIVENESS_DOSAGE", "EFFTIVENESS_DOSAGE_FIRST") \
								.withColumnRenamed("EFFTIVENESS_MANUFACTURER", "EFFTIVENESS_MANUFACTURER_FIRST") \
								.withColumnRenamed("EFFTIVENESS_DOSAGE_SE", "EFFTIVENESS_DOSAGE") \
								.withColumnRenamed("EFFTIVENESS_MANUFACTURER_SE", "EFFTIVENESS_MANUFACTURER") \
								.withColumnRenamed("EFFTIVENESS_PRODUCT_NAME_SE", "EFFTIVENESS_PRODUCT_NAME")
								
	df_second_round.show()
	df_second_round.write.mode("overwrite").parquet(result_path)
	
	return {}


################--------------------- functions ---------------------################
"""
中间文件与结果文件路径
"""
def get_run_id(kwargs):
	run_id = kwargs["run_id"]
	if not run_id:
		run_id = "runid_" + "alfred_runner_test"
	return run_id


def get_job_id(kwargs):
	job_name = kwargs["job_name"]
	job_id = kwargs["job_id"]
	if not job_id:
		job_id = "jobid_" + uuid.uuid4().hex
	return job_name # + "_" + job_id 


def get_result_path(kwargs, run_id, job_id):
	path_prefix = kwargs["path_prefix"]
	return path_prefix + "/" + run_id + "/" + job_id + "/"


def get_depends_file_path(kwargs, job_name, job_key):
	run_id = get_run_id(kwargs)
	return get_result_path(kwargs, run_id, job_name) + job_key
	

def get_depends_path(kwargs):
	depends_lst = eval(kwargs["depend_job_names_keys"])
	result = {}
	for item in depends_lst:
		tmp_lst = item.split("#")
		depends_job = tmp_lst[0]
		depends_key = tmp_lst[1]
		depends_name = tmp_lst[2]
		result[depends_name] = get_depends_file_path(kwargs, depends_job, depends_key)
	return result


"""
读取剂型替换表
"""
def load_dosage_mapping(spark, cpa_dosage_lst_path):
	df_dosage_mapping = spark.read.parquet(cpa_dosage_lst_path)
	return df_dosage_mapping
	

def load_word_dict_encode(spark, word_dict_encode_path):
	df_encode = spark.read.parquet(word_dict_encode_path)
	return df_encode


def second_round_with_col_recalculate(df_second_round, dosage_mapping, df_encode, spark):
	# df_second_round.show()
	# df_second_round.printSchema()
	# dosage_mapping.show()
	df_second_round = df_second_round.join(dosage_mapping, df_second_round.DOSAGE == dosage_mapping.CPA_DOSAGE, how="left").na.fill("")
	# df_second_round = df_second_round.join(dosage_mapping, df_second_round.DOSAGE == dosage_mapping.CHC_DOSAGE, how="left").na.fill("")
	df_second_round = df_second_round.withColumn("MASTER_DOSAGE", when(df_second_round.MASTER_DOSAGE.isNull(), df_second_round.JACCARD_DISTANCE). \
	# df_second_round = df_second_round.withColumn("MASTER_DOSAGE", when(df_second_round.MASTER_DOSAGE.isNull(), array(df_second_round.DOSAGE_STANDARD)). \
						otherwise(df_second_round.MASTER_DOSAGE))
	df_second_round = df_second_round.withColumn("EFFTIVENESS_DOSAGE_SE", dosage_replace(df_second_round.MASTER_DOSAGE, \
														df_second_round.DOSAGE_STANDARD, df_second_round.EFFTIVENESS_DOSAGE)) 
	df_second_round = mnf_encoding_index(df_second_round, df_encode, spark)
	df_second_round = mnf_encoding_cosine(df_second_round)
	df_second_round = df_second_round.withColumn("EFFTIVENESS_MANUFACTURER_SE", \
										when(df_second_round.COSINE_SIMILARITY >= df_second_round.EFFTIVENESS_MANUFACTURER, df_second_round.COSINE_SIMILARITY) \
										.otherwise(df_second_round.EFFTIVENESS_MANUFACTURER))
	df_second_round = mole_dosage_calculaltion(df_second_round)   # 加一列EFF_MOLE_DOSAGE，doubletype
	df_second_round = df_second_round.withColumn("EFFTIVENESS_PRODUCT_NAME_SE", \
								prod_name_replace(df_second_round.EFFTIVENESS_MOLE_NAME, df_second_round.EFFTIVENESS_MANUFACTURER_SE, \
												df_second_round.EFFTIVENESS_PRODUCT_NAME, df_second_round.MOLE_NAME, \
												df_second_round.PRODUCT_NAME_STANDARD, df_second_round.EFF_MOLE_DOSAGE))
												
	return df_second_round
	

@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def dosage_replace(dosage_lst, dosage_standard, eff):

	frame = { "MASTER_DOSAGE": dosage_lst, "DOSAGE_STANDARD": dosage_standard, "EFFTIVENESS_DOSAGE": eff }
	df = pd.DataFrame(frame)

	df["EFFTIVENESS"] = df.apply(lambda x: 1.0 if ((x["DOSAGE_STANDARD"] in x["MASTER_DOSAGE"]) ) \
											else x["EFFTIVENESS_DOSAGE"], axis=1)

	return df["EFFTIVENESS"]
	
	
def mnf_encoding_index(df_cleanning, df_encode, spark):
	# 增加两列MANUFACTURER_NAME_CLEANNING_WORDS MANUFACTURER_NAME_STANDARD_WORDS - array(string)
	# 读取df_lexicon
	df_lexicon = spark.read.parquet("s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/zyyin/lexicon/0.0.1")
	df_pd = df_lexicon.toPandas()  # type = pd.df
	df_cleanning = phcleanning_mnf_seg(df_cleanning, "MANUFACTURER_NAME_STANDARD", "MANUFACTURER_NAME_STANDARD_WORDS")
	df_cleanning = phcleanning_mnf_seg(df_cleanning, "MANUFACTURER_NAME", "MANUFACTURER_NAME_CLEANNING_WORDS")
	df_cleanning = df_cleanning.withColumn("MANUFACTURER_NAME_STANDARD_WORDS_SEG", df_cleanning.MANUFACTURER_NAME_STANDARD_WORDS)
	df_cleanning = df_cleanning.withColumn("MANUFACTURER_NAME_CLEANNING_WORDS_SEG", df_cleanning.MANUFACTURER_NAME_CLEANNING_WORDS)
	df_cleanning = words_to_reverse_index(df_cleanning, df_encode, "MANUFACTURER_NAME_STANDARD_WORDS", "MANUFACTURER_NAME_STANDARD_WORDS")
	df_cleanning = words_to_reverse_index(df_cleanning, df_encode, "MANUFACTURER_NAME_CLEANNING_WORDS", "MANUFACTURER_NAME_CLEANNING_WORDS")
	# df_cleanning.printSchema
	# df_cleanning.show(2)
	df_cleanning = df_cleanning.withColumn("MANUFACTURER_NAME_STANDARD_WORDS", array_distinct(df_cleanning.MANUFACTURER_NAME_STANDARD_WORDS))
	df_cleanning = df_cleanning.withColumn("MANUFACTURER_NAME_CLEANNING_WORDS", array_distinct(df_cleanning.MANUFACTURER_NAME_CLEANNING_WORDS))
	# df_cleanning.show(2)
	
	return df_cleanning
	

def phcleanning_mnf_seg(df_standard, inputCol, outputCol):
	# 2. 英文的分词方法，tokenizer
	# 英文先不管
	# df_standard = df_standard.withColumn("MANUFACTURER_NAME_EN_STANDARD", manifacture_name_en_standify(col("MANUFACTURER_NAME_EN_STANDARD")))
	# df_standard.select("MANUFACTURER_NAME_STANDARD", "MANUFACTURER_NAME_EN_STANDARD").show(truncate=False)
	# tokenizer = Tokenizer(inputCol="MANUFACTURER_NAME_EN_STANDARD", outputCol="MANUFACTURER_NAME_EN_WORDS")
	# df_standard = tokenizer.transform(df_standard)

	# 3. 中文的分词，
	df_standard = df_standard.withColumn("MANUFACTURER_NAME_WORDS", manifacture_name_pseg_cut(col(inputCol)))

	# 4. 分词之后构建词库编码
	# 4.1 stop word remover 去掉不需要的词
	stopWords = ["高新", "化学", "生物", "合资", "中外", "工业", "现代", "化学制品" "科技", "国际", "AU", "OF", "US", "FR", "GE", "FI", "JP", "RO", "CA", "UK", "NO", "IS", "SI", "IT", "JA", \
				"省", "市", "股份", "有限", "总公司", "公司", "集团", "制药", "总厂", "厂", "药业", "责任", "医药", "(", ")", "（", "）", \
				 "有限公司", "股份", "控股", "集团", "总公司", "公司", "有限", "有限责任", "大药厂", '经济特区', '事业所', '株式会社', \
				 "药业", "医药", "制药", "制药厂", "控股集团", "医药集团", "控股集团", "集团股份", "药厂", "分公司", "-", ".", "-", "·", ":", ","]
	remover = StopWordsRemover(stopWords=stopWords, inputCol="MANUFACTURER_NAME_WORDS", outputCol=outputCol)

	return remover.transform(df_standard).drop("MANUFACTURER_NAME_WORDS")
	

@pandas_udf(ArrayType(IntegerType()), PandasUDFType.GROUPED_AGG)
def word_index_to_array(v):
	return v.tolist()


def words_to_reverse_index(df_cleanning, df_encode, inputCol, outputCol):
	df_cleanning = df_cleanning.withColumn("tid", monotonically_increasing_id())
	df_indexing = df_cleanning.withColumn("MANUFACTURER_NAME_STANDARD_WORD_LIST", explode(col(inputCol)))
	df_indexing = df_indexing.join(df_encode, df_indexing.MANUFACTURER_NAME_STANDARD_WORD_LIST == df_encode.WORD, how="left").na.fill(7999)
	df_indexing = df_indexing.groupBy("tid").agg(word_index_to_array(df_indexing.ENCODE).alias("INDEX_ENCODE"))

	df_cleanning = df_cleanning.join(df_indexing, on="tid", how="left")
	df_cleanning = df_cleanning.withColumn(outputCol, df_cleanning.INDEX_ENCODE)
	df_cleanning = df_cleanning.drop("tid", "INDEX_ENCODE", "MANUFACTURER_NAME_STANDARD_WORD_LIST")
	return df_cleanning
	

def mnf_encoding_cosine(df_cleanning):
	df_cleanning = df_cleanning.withColumn("COSINE_SIMILARITY", \
					mnf_index_word_cosine_similarity(df_cleanning.MANUFACTURER_NAME_CLEANNING_WORDS, df_cleanning.MANUFACTURER_NAME_STANDARD_WORDS))
	return df_cleanning
	

@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def mnf_index_word_cosine_similarity(o, v):
	frame = {
		"CLEANNING": o,
		"STANDARD": v
	}
	df = pd.DataFrame(frame)
	def array_to_vector(arr):
		idx = []
		values = []
		# try:
		if type(arr) != np.ndarray:
			s = [8999,]
		# except AttributeError:
		else:
			s = list(set(arr))
		s.sort()
		for item in s:
			if isnan(item):
				idx.append(8999)
				values.append(1)
				break
			else:
				idx.append(item)
				if item < 2000:
					values.append(2)
				elif (item >= 2000) & (item < 5000):
					values.append(10)
				else:
					values.append(1)
		return Vectors.sparse(9000, idx, values)
		#                    (向量长度，索引数组，与索引数组对应的数值数组)
	def cosine_distance(u, v):
		u = u.toArray()
		v = v.toArray()
		return float(np.dot(u, v) / (sqrt(np.dot(u, u)) * sqrt(np.dot(v, v))))
	df["CLENNING_FEATURE"] = df["CLEANNING"].apply(lambda x: array_to_vector(x))
	df["STANDARD_FEATURE"] = df["STANDARD"].apply(lambda x: array_to_vector(x))
	df["RESULT"] = df.apply(lambda x: cosine_distance(x["CLENNING_FEATURE"], x["STANDARD_FEATURE"]), axis=1)
	return df["RESULT"]
	
	
def mole_dosage_calculaltion(df):
	@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
	def pudf_jws_func_tmp(s1, s2):
		frame = {
			"LEFT": s1,
			"RIGHT": s2
		}
		df = pd.DataFrame(frame)
		
		df["RESULT"] = df.apply(lambda x: jaro_winkler_similarity(x["LEFT"], x["RIGHT"]), axis=1)
		return df["RESULT"]
		
	# 给df 增加一列：EFF_MOLE_DOSAGE
	df_dosage_explode = df.withColumn("MASTER_DOSAGES", explode("MASTER_DOSAGE"))
	df_dosage_explode = df_dosage_explode.withColumn("MOLE_DOSAGE", concat(df_dosage_explode.MOLE_NAME, df_dosage_explode.MASTER_DOSAGES))
	df_dosage_explode = df_dosage_explode.withColumn("jws", pudf_jws_func_tmp(df_dosage_explode.MOLE_DOSAGE, df_dosage_explode.PRODUCT_NAME_STANDARD))
	df_dosage_explode = df_dosage_explode.groupBy('id').agg({"jws":"max"}).withColumnRenamed("max(jws)","EFF_MOLE_DOSAGE")
	df_dosage = df.join(df_dosage_explode, "id", how="left")
	
	return df_dosage


@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def prod_name_replace(eff_mole_name, eff_mnf_name, eff_product_name, mole_name, prod_name_standard, eff_mole_dosage):
	frame = { "EFFTIVENESS_MOLE_NAME": eff_mole_name, "EFFTIVENESS_MANUFACTURER_SE": eff_mnf_name, "EFFTIVENESS_PRODUCT_NAME": eff_product_name,
			  "MOLE_NAME": mole_name, "PRODUCT_NAME_STANDARD": prod_name_standard, "EFF_MOLE_DOSAGE": eff_mole_dosage,}
	df = pd.DataFrame(frame)

	df["EFFTIVENESS_PROD"] = df.apply(lambda x: max((0.5* x["EFFTIVENESS_MOLE_NAME"] + 0.5* x["EFFTIVENESS_MANUFACTURER_SE"]), \
									# (x["EFFTIVENESS_PRODUCT_NAME"])), axis=1)
								(x["EFFTIVENESS_PRODUCT_NAME"]), \
								(jaro_winkler_similarity(x["MOLE_NAME"], x["PRODUCT_NAME_STANDARD"])), \
								(x["EFF_MOLE_DOSAGE"])), axis=1)

	return df["EFFTIVENESS_PROD"]


def spec_split_matching(df):
	# df.printSchema()
	
	df = df.withColumn("SPEC_valid_total_STANDARD",  spec_valid_std_transfer_pandas_udf(df.SPEC_valid_digit_STANDARD))
	# df = df.where(df.SPEC_STANDARD == "84.5UG  /DOS  60")
	
	df = df.withColumn("SPEC", regexp_replace("SPEC", r"(微克)", "UG"))
	df = df.withColumn("SPEC", regexp_replace("SPEC", r"[()]", ""))
	df = df.withColumn("SPEC", regexp_replace("SPEC", r"(（)", ""))
	df = df.withColumn("SPEC", regexp_replace("SPEC", r"(）)", ""))
	df = df.withColumn("SPEC", regexp_replace("SPEC", r"(Μ)", "M"))
	# df = df.drop("SPEC_valid_digit_STANDARD", "SPEC_valid_unit_STANDARD", "SPEC_gross_digit_STANDARD", "SPEC_gross_unit_STANDARD", "SPEC_STANDARD")

	
	df = df.withColumn("SPEC", regexp_replace("SPEC", r"(万)", "T"))
	df = df.withColumn("SPEC", regexp_replace("SPEC", r"(μ)", "U"))
	df = df.withColumn("SPEC", upper(df.SPEC))
	df = df.replace(" ", "")
	# df = df.where(df.SPEC_STANDARD == "62.5UG+25UG/DOS 30")
	# df = df.withColumn("SPEC_gross", regexp_extract('SPEC', spec_regex, 2))
	# 拆分规格的成分
	# df = df.withColumn("SPEC_percent", regexp_extract('SPEC', r'(\d*.*\d+%)', 1))
	# df = df.withColumn("SPEC_co", regexp_extract('SPEC', r'(CO)', 1))
	spec_valid_regex =  r'([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)'
	df = df.withColumn("SPEC_valid", regexp_extract('SPEC', spec_valid_regex, 1))
	spec_gross_regex =  r'([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)[ ,/:∶+\s]*[\u4e00-\u9fa5]*([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)'
	df = df.withColumn("SPEC_gross", regexp_extract('SPEC', spec_gross_regex, 2))
	
	spec_valid_se_regex =  r'([0-9]\d*\.?\d*\s*[:/+][0-9]\d*\.?\d*\s*[A-Za-z]+)'
	df = df.withColumn("SPEC_valid_2", regexp_extract('SPEC', spec_valid_se_regex, 1))
	df = df.withColumn("SPEC_valid", when((df.SPEC_valid_2 != ""), df.SPEC_valid_2).otherwise(df.SPEC_valid))
	# df = df.drop("SPEC_valid_2")
	
	# spec_third_regex = r'([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)[ /:∶+\s]([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)[ /:∶+\s]([0-9]\d*\.?\d*\s*[A-Za-z]*/?\s*[A-Za-z]+)'
	# df = df.withColumn("SPEC_third", regexp_extract('SPEC', spec_third_regex, 3))

	# pure_number_regex_spec = r'(\s\d+$)'
	# df = df.withColumn("SPEC_pure_number", regexp_extract('SPEC', pure_number_regex_spec, 1))
	# dos_regex_spec = r'(/DOS)'
	# df = df.withColumn("SPEC_dos", regexp_extract('SPEC', dos_regex_spec, 1))

	
	# df = df.withColumn("SPEC_valid", dos_pandas_udf(df.SPEC_valid, df.SPEC_pure_number, df.SPEC_dos))
	
	# 单位转换
	df = df.withColumn("SPEC_valid", transfer_unit_pandas_udf(df.SPEC_valid))
	df = df.withColumn("SPEC_gross", transfer_unit_pandas_udf(df.SPEC_gross))
	df = df.drop("SPEC_gross_digit", "SPEC_gross_unit", "SPEC_valid_digit", "SPEC_valid_unit")
	# df = df.withColumn("SPEC_percent", percent_pandas_udf(df.SPEC_percent, df.SPEC_valid, df.SPEC_gross))
	
	df = df.na.fill("")
	df.show()
	
	# 把百分号补充到有效成分列中
	# df = df.withColumn("SPEC_percent", lit(""))
	# df = df.withColumn("SPEC_gross", when(((df.SPEC_gross == "") & (df.SPEC_valid != "")), df.SPEC_valid).otherwise(df.SPEC_gross))
	# df = df.withColumn("SPEC_valid", when((df.SPEC_percent != ""), df.SPEC_percent).otherwise(df.SPEC_valid))
	# df = df.withColumn("SPEC_valid", when((df.SPEC_valid == df.SPEC_gross), lit("")).otherwise(df.SPEC_valid))
	
	# 拆分成四列
	# digit_regex_spec = r'(\d+\.?\d*e?-?\d*?)'
	
	# df = df.withColumn("SPEC_valid_digit", regexp_extract('SPEC_valid', digit_regex_spec, 1))
	# df = df.withColumn("SPEC_valid_unit", regexp_replace('SPEC_valid', digit_regex_spec, ""))
	
	# df = df.withColumn("SPEC_gross_digit", regexp_extract('SPEC_gross', digit_regex_spec, 1))
	# df = df.withColumn("SPEC_gross_unit", regexp_replace('SPEC_gross', digit_regex_spec, ""))
	# df = df.na.fill("")
	# df.show()
	
	unit_regex_spec = r'([A-Z]+\d*)'
	
	df = df.withColumn("SPEC_valid_unit", regexp_extract('SPEC_valid', unit_regex_spec, 1))
	df = df.withColumn("SPEC_valid_digit", regexp_replace('SPEC_valid', unit_regex_spec, ""))
	
	df = df.withColumn("SPEC_gross_unit", regexp_extract('SPEC_gross', unit_regex_spec, 1))
	df = df.withColumn("SPEC_gross_digit", regexp_replace('SPEC_gross', unit_regex_spec, ""))
	df = df.withColumn("SPEC_valid_total_ORIGINAL",  spec_valid_std_transfer_pandas_udf(df.SPEC_valid_digit))
	df = df.withColumn("SPEC_total_ORIGINAL",  spec_total_cleanning_pandas_udf(df.SPEC_valid_digit, df.SPEC_valid_unit, df.SPEC_gross_digit, df.SPEC_gross_unit))
	
	df = df.na.fill("")
	df.select("SPEC_valid_digit", "SPEC_valid_total_ORIGINAL", "SPEC_valid_digit_STANDARD", "SPEC_valid_total_STANDARD").show()
	# df.show()
	
	# 开始计算effectiveness的逻辑
	df = df.withColumn("EFFTIVENESS_SPEC_SPLIT", lit(0))
	# 1. 如果 【四列】分别都相等，则eff为1
	df = df.withColumn("EFFTIVENESS_SPEC_SPLIT", when( \
						((df.SPEC_valid_digit_STANDARD == df.SPEC_valid_digit) & (df.SPEC_valid_unit_STANDARD == df.SPEC_valid_unit) \
						& (df.SPEC_gross_digit_STANDARD == df.SPEC_gross_digit) & (df.SPEC_gross_unit_STANDARD == df.SPEC_gross_unit)), \
						lit(1))\
						.otherwise(df.EFFTIVENESS_SPEC_SPLIT))
	# df = df.where(df.EFFTIVENESS_SPEC_SPLIT == 0)
	
	# 2. 如果original/standard【只有valid/只有gross】，且仅有的部分是可以对应的，则eff为1
	df = df.withColumn("EFFTIVENESS_SPEC_SPLIT", when( \
						((df.SPEC_valid_digit == "") & (df.SPEC_valid_unit == "") \
						& ((df.SPEC_gross_digit_STANDARD == df.SPEC_gross_digit) & (df.SPEC_gross_unit_STANDARD == df.SPEC_gross_unit)) \
						| ((df.SPEC_valid_digit_STANDARD == df.SPEC_gross_digit) & (df.SPEC_valid_unit_STANDARD == df.SPEC_gross_unit))), \
						lit(1))\
						.otherwise(df.EFFTIVENESS_SPEC_SPLIT))
	df = df.withColumn("EFFTIVENESS_SPEC_SPLIT", when( \
						((df.SPEC_gross_digit == "") & (df.SPEC_gross_unit == "") \
						& ((df.SPEC_valid_digit_STANDARD == df.SPEC_valid_digit) & (df.SPEC_valid_unit_STANDARD == df.SPEC_valid_unit)) \
						| ((df.SPEC_gross_digit_STANDARD == df.SPEC_valid_digit) & (df.SPEC_gross_unit_STANDARD == df.SPEC_valid_unit))), \
						lit(1))\
						.otherwise(df.EFFTIVENESS_SPEC_SPLIT))
						
	# 3.如果【总量两列】 = 【有效成分两列】& 【有效成分两列】= 【总量两列】，则eff为1
	df = df.withColumn("EFFTIVENESS_SPEC_SPLIT", when( \
						((df.SPEC_valid_digit_STANDARD == df.SPEC_gross_digit) & (df.SPEC_valid_unit_STANDARD == df.SPEC_gross_unit) \
						& (df.SPEC_gross_digit_STANDARD == df.SPEC_valid_digit) & (df.SPEC_gross_unit_STANDARD == df.SPEC_valid_unit)), \
						lit(1))\
						.otherwise(df.EFFTIVENESS_SPEC_SPLIT))
						
	# 4. 如果【源数据valid == 标准数据valid之和】，则eff为1
	df = df.withColumn("EFFTIVENESS_SPEC_SPLIT", when( \
						((df.SPEC_valid_total_STANDARD == df.SPEC_valid_digit) & (df.SPEC_valid_unit_STANDARD == df.SPEC_valid_unit)), \
						lit(1))\
						.otherwise(df.EFFTIVENESS_SPEC_SPLIT))
	# df.withColumn("SPEC_valid_total_STANDARD", when(df.SPEC_valid_total_STANDARD == "nan", lit("")).otherwise(df.SPEC_valid_total_STANDARD))
	# df = df.where(df.SPEC == "CO 1.003 GM")
	# 5. 一些骚操作（目前是针对azsanofi的）：
	# 如果 【源数据有效成分 == 标准有效成分的取整值/四舍五入值】，则eff为0.99
	df = df.withColumn("EFFTIVENESS_SPEC_SPLIT", when(df.EFFTIVENESS_SPEC_SPLIT == 1, df.EFFTIVENESS_SPEC_SPLIT) \
												.otherwise(spec_eff_int_or_carry(df.SPEC_valid_digit_STANDARD, df.SPEC_valid_total_ORIGINAL, df.SPEC_valid_unit_STANDARD, \
																	df.SPEC_valid_unit, df.SPEC_valid_total_STANDARD, df.EFFTIVENESS_SPEC_SPLIT)))
	# df = df.withColumn("EFFTIVENESS_SPEC_SPLIT", when(df.SPEC.contains("162.5"), lit(0.999999)). \
	# 											otherwise(df.EFFTIVENESS_SPEC_SPLIT ))
	
	# 6. 如果【标准总量/标准有效成分 == 源数据总+有效】，则eff为1
	df = df.withColumn("EFFTIVENESS_SPEC_SPLIT", when( \
						(((df.SPEC_total_ORIGINAL == df.SPEC_gross_digit_STANDARD) & (df.SPEC_gross_unit == df.SPEC_gross_unit_STANDARD)) \
						| ((df.SPEC_total_ORIGINAL == df.SPEC_valid_digit_STANDARD) & (df.SPEC_valid_unit == df.SPEC_valid_unit_STANDARD))), \
						lit(1)) \
						.otherwise(df.EFFTIVENESS_SPEC_SPLIT))
	# df.show()
	# df.select("SPEC", "SPEC_STANDARD", "EFFTIVENESS_SPEC_SPLIT").show(25)
	
	return df


@pandas_udf(StringType(), PandasUDFType.SCALAR)
def spec_valid_std_transfer_pandas_udf(value):
	
	def digit_addition(spec_str):
		lst = spec_str.split(",")
		value_total = 0.0
		# if (lst[0] == "nan") | (len(lst) == 0):
		if len(lst) <= 1:
			value = "0.0"
		else:
			# try:
			for num in lst:
				num = float(num)
				value_total = (value_total + num)
				value  = str(value_total)
			# except:
			# 	value = 0.0
		return value
				
	frame = { "SPEC": value }
	df = pd.DataFrame(frame)
	df["RESULT"] = df["SPEC"].apply(digit_addition)
	return df["RESULT"]


@pandas_udf(StringType(), PandasUDFType.SCALAR)
def transfer_unit_pandas_udf(value):
	def unit_trans(value, unit):
		# value transform
		if unit == "G" or unit == "GM":
			value = value *1000
		elif unit == "UG" or unit == "UG/DOS":
			value = value /1000
		elif unit == "L":
			value = value *1000
		elif unit == "TU" or unit == "TIU":
			value = value *10000
		elif unit == "MU" or unit == "MIU" or unit == "M":
			value = value *1000000
		elif (unit == "Y"):
			value = value /1000
		if value >= 1:
			value = round(value, 1)
		else:
			value = value

		# unit transform
		unit_switch = {
				"G": "MG",
				"GM": "MG",
				"MG": "MG",
				"UG": "MG",
				"L": "ML",
				"AXAU": "U",
				"AXAIU": "U",
				"IU": "U",
				"TU": "U",
				"TIU": "U",
				"MU": "U",
				"MIU": "U",
				"M": "U",
				"Y": "MG",
				"MC": "MC",
			}
		try:
			unit = unit_switch[unit]
		except KeyError:
			pass
		return value, unit
	
	
	def unit_transform(spec_str):
		spec_str = spec_str.replace(" ", "")
		# 拆分数字和单位
		digit_regex = '\d+\.?\d*e?-?\d*?'
		# digit_regex = '0.\d*'
		# try:
		if spec_str != "":
			values = re.findall(digit_regex, spec_str)
			if len(values) == 1:
				value = values[0]
				unit = spec_str.strip(value)  # type = str
				value = float(value)  # type = float
				value = unit_trans(value, unit)[0]
				unit = unit_trans(value, unit)[1]
			elif len(values) >= 2:
				# unit = unit
				# value = 12222
				value_result = ""
				unit_regex = '[A-Z]+\d*'
				unit = re.findall(unit_regex, spec_str)[0]
				# value = "000"
				for value in values:
					value = float(value)  # type = float
					value = unit_trans(value, unit)[0]
					value_result = value_result + str(value) + ","
				unit = unit_trans(value, unit)[1]
				value = value_result.strip(",")
				
		else:
			unit = ""
			value = ""

		return str(value) + unit

		# except Exception:
		# 	return spec_str

	frame = { "SPEC": value }
	df = pd.DataFrame(frame)
	df["RESULT"] = df["SPEC"].apply(unit_transform)
	return df["RESULT"]
	
	
"""
处理分词字典
"""
dict_seg = {}

def get_seg(spark, lexicon_path):
	lexicon = spark.read.csv(lexicon_path,header=True)
	lexicon = lexicon.rdd.map(lambda x: x.lexicon).collect()
	seg = pkuseg.pkuseg(user_dict=lexicon)
	dict_seg['seg'] = seg
	# print(lexicon)
	return dict_seg
	

@pandas_udf(ArrayType(StringType()), PandasUDFType.SCALAR)
def manifacture_name_pseg_cut(mnf):
	frame = {
		"MANUFACTURER_NAME_STANDARD": mnf,
	}
	df = pd.DataFrame(frame)
	seg = dict_seg['seg']
	df["MANUFACTURER_NAME_STANDARD_WORDS"] = df["MANUFACTURER_NAME_STANDARD"].apply(lambda x: seg.cut(x))
	return df["MANUFACTURER_NAME_STANDARD_WORDS"]
	
	
@pandas_udf(StringType(), PandasUDFType.SCALAR)
def spec_total_cleanning_pandas_udf(SPEC_valid_digit, SPEC_valid_unit, SPEC_gross_digit, SPEC_gross_unit):
	
	def digit_addition(a, b, valid_unit, gross_unit):
		if (valid_unit == gross_unit) & ("," not in a) & (a != ""):
			a = float(a)
			b = float(b)
			return str(a+b)
		else:
			return ""
				
	frame = { "SPEC_valid_digit": SPEC_valid_digit, "SPEC_valid_unit": SPEC_valid_unit,
			  "SPEC_gross_digit": SPEC_gross_digit, "SPEC_gross_unit": SPEC_gross_unit,	}
	df = pd.DataFrame(frame)
	df["RESULT"] = df.apply(lambda x: digit_addition(x["SPEC_valid_digit"], x["SPEC_gross_digit"], x["SPEC_valid_unit"], x["SPEC_gross_unit"]), axis=1)
	return df["RESULT"]

@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def spec_eff_int_or_carry(SPEC_valid_digit_STANDARD, SPEC_valid_total_ORIGINAL, SPEC_valid_unit_STANDARD, SPEC_valid_unit, SPEC_valid_total_STANDARD, EFFTIVENESS_SPEC_SPLIT):

	frame = { "SPEC_valid_digit_STANDARD": SPEC_valid_digit_STANDARD, "SPEC_valid_total_ORIGINAL": SPEC_valid_total_ORIGINAL,
			  "SPEC_valid_unit_STANDARD": SPEC_valid_unit_STANDARD,  "SPEC_valid_unit": SPEC_valid_unit, 
			  "SPEC_valid_total_STANDARD": SPEC_valid_total_STANDARD, "EFFTIVENESS_SPEC_SPLIT": EFFTIVENESS_SPEC_SPLIT}
	df = pd.DataFrame(frame)
	
	
	# try:
	df["EFFTIVENESS_SPEC_SPLIT"] = df.apply(lambda x: 0.99 if ((int(float(x["SPEC_valid_total_ORIGINAL"])) == int(float(x["SPEC_valid_total_STANDARD"]))) \
														& (x["SPEC_valid_unit_STANDARD"] == x["SPEC_valid_unit"])) \
													else 0.999 if ((math.ceil(float(x["SPEC_valid_total_ORIGINAL"])) == math.ceil(float(x["SPEC_valid_total_STANDARD"]))) \
														& (x["SPEC_valid_unit_STANDARD"] == x["SPEC_valid_unit"])) \
											else x["EFFTIVENESS_SPEC_SPLIT"], axis=1)
	# except ValueError:
	# 	df["EFFTIVENESS_SPEC_SPLIT"] = df.apply(lambda x: 0.888, axis=1)

	return df["EFFTIVENESS_SPEC_SPLIT"]
################-----------------------------------------------------################