# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""

from phcli.ph_logs.ph_logs import phs3logger
import uuid
import pandas as pd
import numpy as np
import pkuseg
import math
from math import isnan, sqrt
from pyspark.sql.types import *
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import col, monotonically_increasing_id, explode
from pyspark.sql.functions import when
from pyspark.sql.functions import array_distinct, array
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import StopWordsRemover
from nltk.metrics.distance import jaro_winkler_similarity


def execute(**kwargs):
	"""
		please input your code below
		get spark session: spark = kwargs["spark"]()
	"""
##################=========configure===========###################
	logger = phs3logger(kwargs["job_id"])
	spark = kwargs["spark"]()
	logger.info(kwargs)
##################=========configure===========###################

############-----------input-------------------------###################
	depends = get_depends_path(kwargs)
	path_effective_result = depends["input"]
	g_repartition_shared = int(kwargs["g_repartition_shared"])
	word_dict_encode_path = kwargs["word_dict_encode_path"]
	lexicon_path = kwargs["lexicon_path"]	
############-----------input-------------------------###################

###########------------output------------------------###################
	job_id = get_job_id(kwargs)
	run_id = get_run_id(kwargs)
	result_path_prefix = get_result_path(kwargs, run_id, job_id)
	result_path = result_path_prefix + kwargs["mnf_adjust_result"]
	mid_path= result_path_prefix + kwargs["mnf_adjust_mid"]
###########------------output------------------------###################

###############--------loading files --------------##################
	get_seg(spark, lexicon_path)
	df_second_round = load_effective_result(spark, path_effective_result)
	df_encode = load_word_dict_encode(spark, word_dict_encode_path)
###############--------loading files----------------#################


#########--------------main function--------------------#################  
	df_second_round = second_round_with_col_recalculate(df_second_round, df_encode, spark)

# 	df_second_round.repartition(g_repartition_shared).write.mode("overwrite").parquet(mid_path)
    #选取指定的列用于和adjust_spec job 进行union操作
	df_second_round = select_specified_cols(df_second_round)

	df_second_round.repartition(g_repartition_shared).write.mode("overwrite").parquet(result_path)
#########--------------main function--------------------#################  
	
   
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

def load_effective_result(spark, path_effective_result):
	df_second_round = spark.read.parquet(path_effective_result)
	return df_second_round

"""
读取剂型替换表
"""
def load_word_dict_encode(spark, word_dict_encode_path):
	df_encode = spark.read.parquet(word_dict_encode_path)
	return df_encode


def second_round_with_col_recalculate(df_second_round, df_encode, spark):
	df_second_round = mnf_encoding_index(df_second_round, df_encode, spark)
	df_second_round = mnf_encoding_cosine(df_second_round)
	df_second_round = df_second_round.withColumn("EFFTIVENESS_MANUFACTURER_SE", \
										when(df_second_round.COSINE_SIMILARITY >= df_second_round.EFFTIVENESS_MANUFACTURER, df_second_round.COSINE_SIMILARITY) \
										.otherwise(df_second_round.EFFTIVENESS_MANUFACTURER))
	df_second_round = df_second_round.withColumn("EFFTIVENESS_PRODUCT_NAME_SE", \
								prod_name_replace(df_second_round.EFFTIVENESS_MOLE_NAME, df_second_round.EFFTIVENESS_MANUFACTURER_SE, \
												df_second_round.EFFTIVENESS_PRODUCT_NAME, df_second_round.MOLE_NAME, \
												df_second_round.PRODUCT_NAME_STANDARD))
        
	df_second_round = df_second_round.withColumnRenamed("EFFTIVENESS_PRODUCT_NAME", "EFFTIVENESS_PRODUCT_NAME_FIRST") \
								.withColumnRenamed("EFFTIVENESS_MANUFACTURER", "EFFTIVENESS_MANUFACTURER_FIRST") \
								.withColumnRenamed("EFFTIVENESS_DOSAGE_SE", "EFFTIVENESS_DOSAGE") \
								.withColumnRenamed("EFFTIVENESS_MANUFACTURER_SE", "EFFTIVENESS_MANUFACTURER") \
								.withColumnRenamed("EFFTIVENESS_PRODUCT_NAME_SE", "EFFTIVENESS_PRODUCT_NAME")
								# .withColumnRenamed("EFFTIVENESS_DOSAGE", "EFFTIVENESS_DOSAGE_FIRST") \
	df_second_round.persist()	
	return df_second_round


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
	
    
    
    
def select_specified_cols(df_second_round):
	print(df_second_round.columns)
	cols = ["SID", "ID","PACK_ID_CHECK",  "PACK_ID_STANDARD","DOSAGE","MOLE_NAME","PRODUCT_NAME","SPEC","PACK_QTY","MANUFACTURER_NAME","SPEC_ORIGINAL",
			"MOLE_NAME_STANDARD","PRODUCT_NAME_STANDARD","CORP_NAME_STANDARD","MANUFACTURER_NAME_STANDARD","MANUFACTURER_NAME_EN_STANDARD","DOSAGE_STANDARD","SPEC_STANDARD","PACK_QTY_STANDARD",
			"SPEC_valid_digit_STANDARD","SPEC_valid_unit_STANDARD","SPEC_GROSS_VALUE_PURE_STANDARD","SPEC_GROSS_UNIT_PURE_STANDARD","SPEC_GROSS_VALUE_PURE","CHC_GROSS_UNIT","SPEC_VALID_VALUE_PURE",
			"SPEC_VALID_UNIT_PURE","EFFTIVENESS_MOLE_NAME","EFFTIVENESS_PRODUCT_NAME","EFFTIVENESS_DOSAGE","EFFTIVENESS_PACK_QTY","EFFTIVENESS_MANUFACTURER","EFFTIVENESS_SPEC"]
	df_second_round = df_second_round.select(cols)  
	return df_second_round

def mnf_encoding_index(df_cleanning, df_encode, spark):

	df_cleanning = phcleanning_mnf_seg(df_cleanning, "MANUFACTURER_NAME_STANDARD", "MANUFACTURER_NAME_STANDARD_WORDS")
	df_cleanning = phcleanning_mnf_seg(df_cleanning, "MANUFACTURER_NAME", "MANUFACTURER_NAME_CLEANNING_WORDS")
	df_cleanning = df_cleanning.withColumn("MANUFACTURER_NAME_STANDARD_WORDS_SEG", df_cleanning.MANUFACTURER_NAME_STANDARD_WORDS)
	df_cleanning = df_cleanning.withColumn("MANUFACTURER_NAME_CLEANNING_WORDS_SEG", df_cleanning.MANUFACTURER_NAME_CLEANNING_WORDS)
	df_cleanning = words_to_reverse_index(df_cleanning, df_encode, "MANUFACTURER_NAME_STANDARD_WORDS", "MANUFACTURER_NAME_STANDARD_WORDS")
	df_cleanning = words_to_reverse_index(df_cleanning, df_encode, "MANUFACTURER_NAME_CLEANNING_WORDS", "MANUFACTURER_NAME_CLEANNING_WORDS")
	df_cleanning = df_cleanning.withColumn("MANUFACTURER_NAME_STANDARD_WORDS", array_distinct(df_cleanning.MANUFACTURER_NAME_STANDARD_WORDS))
	df_cleanning = df_cleanning.withColumn("MANUFACTURER_NAME_CLEANNING_WORDS", array_distinct(df_cleanning.MANUFACTURER_NAME_CLEANNING_WORDS))
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
				"省", "市", "股份", "有限",  "公司", "集团", "制药", "总厂", "厂", "责任", "医药", "(", ")", "（", "）", \
				 "有限公司", "控股", "总公司", "有限", "有限责任", "大药厂", '经济特区', '事业所', '株式会社', \
				 "药业", "制药", "制药厂", "医药集团", "控股集团", "集团股份", "药厂", "分公司", "-", ".", "-", "·", ":", ","]
	remover = StopWordsRemover(stopWords=stopWords, inputCol="MANUFACTURER_NAME_WORDS", outputCol=outputCol)

	return remover.transform(df_standard).drop("MANUFACTURER_NAME_WORDS")
	

@pandas_udf(ArrayType(IntegerType()), PandasUDFType.GROUPED_AGG)
def word_index_to_array(v):
	return v.tolist()


def words_to_reverse_index(df_cleanning, df_encode, inputCol, outputCol):
	df_cleanning = df_cleanning.withColumn("tid", monotonically_increasing_id())
	df_indexing = df_cleanning.withColumn("MANUFACTURER_NAME_STANDARD_WORD_LIST", explode(col(inputCol)))
	df_indexing = df_indexing.join(df_encode, df_indexing.MANUFACTURER_NAME_STANDARD_WORD_LIST == df_encode.WORD, how="left").na.fill(8999)
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


@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def prod_name_replace(eff_mole_name, eff_mnf_name, eff_product_name, mole_name, prod_name_standard):
	frame = { "EFFTIVENESS_MOLE_NAME": eff_mole_name, "EFFTIVENESS_MANUFACTURER_SE": eff_mnf_name, "EFFTIVENESS_PRODUCT_NAME": eff_product_name,
			  "MOLE_NAME": mole_name, "PRODUCT_NAME_STANDARD": prod_name_standard, }
	df = pd.DataFrame(frame)

	df["EFFTIVENESS_PROD"] = df.apply(lambda x: max((0.5* x["EFFTIVENESS_MOLE_NAME"] + 0.5* x["EFFTIVENESS_MANUFACTURER_SE"]), \
								(x["EFFTIVENESS_PRODUCT_NAME"]), \
								(jaro_winkler_similarity(x["MOLE_NAME"], x["PRODUCT_NAME_STANDARD"]))), axis=1)

	return df["EFFTIVENESS_PROD"]

################-----------------------------------------------------################