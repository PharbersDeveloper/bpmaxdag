# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

功能描述：job4: one hot matrix 计算生产厂家相似度
  * @author yzy
  * @version 0.0
  * @since 2020/10/12
  * @note 输入数据：cpa_prod_join_data（根据mole_name，一条cpa数据join出来很多条与prod的匹配数据）
		  落盘数据：cpa_ed （ed_total列是总编辑距离）
  
"""


# from phlogs.phlogs import phlogger
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
# from pyspark.sql import functions as func
from pyspark.sql.functions import col, pandas_udf
import re
import numpy as np
import synonyms
from progressbar import *
# import pickle
import pandas as pd
import pkuseg


def execute(out_path, in_cpa_path, in_prod_path):
	"""
		please input your code below
	"""
	
	print("--"*80)
	print("程序start: job4_1_one_hot")
	
	os.environ["PYSPARK_PYTHON"] = "python3"
	# 读取s3桶中的数据
	spark = SparkSession.builder \
		.master("yarn") \
		.appName("BPBatchDAG") \
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
		
	
	# 需要的所有表格命名
	cpa_prod_join_data = spark.read.parquet(out_path + "/" + "cpa_prod_join")
	# cpa_prod_join_data.show()
	# print(cpa_prod_join_data.count())  # 37173
	
	# prod表：
	prod = spark.read.parquet(in_prod_path)
	# prod.show(3)
	# print(prod.count())   # 44161
	
	# cpa源数据：
	cpa_input_data = spark.read.parquet(in_cpa_path).drop("id")
	# cpa_input_data.show(3)
	# print(cpa_input_data.count())  # 17252
	
	# 创建生产厂家名字的list并去重
	row_cpa = cpa_input_data.select('MANUFACTURER_NAME').collect()
	cpa_mnf_lst = [row.MANUFACTURER_NAME for row in row_cpa]
	cpa_mnf_lst = list(set(cpa_mnf_lst))
	# print(len(cpa_mnf_lst))  # 1938
	row_prod = prod.select('MNF_NAME_CH').collect()
	prod_mnf_lst = [row.MNF_NAME_CH for row in row_prod]
	prod_mnf_lst = list(set(prod_mnf_lst))
	# print(len(prod_mnf_lst))   # 2697
	mnf_lst = list(set(cpa_mnf_lst + prod_mnf_lst))
	# print(mnf_lst)
	# print(len(mnf_lst))  # 3545
	
	# @func.udf(returnType=FloatType())
	def one_hot(in_word1, in_word2):
		print("------------------开始建立wordDict词库------------------")
	
		seg = pkuseg.pkuseg()

		def load_obj(name):
			# 反序列化对象，将文件中的数据解析为一个python对象,返回值是一个dict，key是词语，value是数字1234...不重复
			with open(name + '.pkl', 'rb') as f:  # rb: 以二进制格式打开一个文件用于只读
				return pickle.load(f)
		
		def save_obj(obj, name):
			with open(name + '.pkl', 'wb') as f:
				pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
	
	
		def build_word_dict(strList):
			# 将入参 strList 里面的分词写入返回值 wordDict（就按照【是一个dict，key是词语，value是数字1234...不重复】这样的格式）
			wordDict = {}
			widgets = ['Progress bulid word dict: ', Percentage(), ' ', Bar('#'), ' ', Timer(),
					   ' ', ETA(), ' ', FileTransferSpeed()]
			pbar = ProgressBar(widgets=widgets, maxval=10 * len(strList)).start()
			for i in range(len(strList)):
				pbar.update(10 * i + 1)
				time.sleep(0.0001)
				for j in range(len(seg.cut(strList[i]))):
					if seg.cut(strList[i])[j] not in wordDict:
						wordDict.update({seg.cut(strList[i])[j]: len(wordDict) + 1})
			pbar.finish()
			return wordDict
			
		wordList = mnf_lst
	
		
		# 2. 创建 词库字典  （3/建立词库）
		wordDict = build_word_dict(wordList)  # todo：不用pickle行不行
		# 如果 chiWordDict.pkl 存在，直接load，储存在wordDict 里面
		# if os.path.exists(".chiWordDict.pkl"):
		# 	print("word dict exists")
		# 	wordDict = load_obj(".chiWordDict")
		# 	# wordDict：
		# 	# {'安徽': 1, '丰原': 2, '药业': 3, '股份': 4, '有限': 5, '公司': 6, '上海': 7, '衡山': 8, '复星': 9, '新亚': 10, '闵行': 11, '信谊': 12,
		# 	# '天平': 13, 。。。。。。。。 '康赐尔': 2805, '万翔': 2806, '奥罗历加': 2807, '欧洲': 2808, '瑞得': 2809, '合通': 2810,
		# 	# '新时代': 2811, '启天': 2812, '卢森堡': 2813, '丰生': 2814, '娃哈哈': 2815, '保健品': 2816, '葡立': 2817, '龙州': 2818, '德泽': 2819,
		# 	# '台裕': 2820}
		# # 如果 chiWordDict.pkl 不存在，用 ProgressBar 来 build_word_dict，根据 wordList 的内容自己创建wordDict
		# else:
		# 	print("build word dict")
		# 	wordDict = build_word_dict(wordList)
			# save_obj(wordDict, ".chiWordDict")
		
		print("word dict ready")
	
		print("------------------开始建立 one hot 索引------------------")
		
		def build_word_index(wordList, wordDict):
		    widgets = ['Progress build str index: ', Percentage(), ' ', Bar('#'), ' ', Timer(),
		               ' ', ETA(), ' ', FileTransferSpeed()]
		    max1 = len(wordDict)
		    # 建立一个numpy array，shape 形状是根据 wordDict wordList 的长度来确定的
		    array = np.zeros((max1, len(wordList)))
		    pbar = ProgressBar(widgets=widgets, maxval=10 * len(wordList)).start()
		    for i in range(len(wordList)):
		        pbar.update(10 * i + 1)
		        time.sleep(0.0001)
		        tempseg = seg.cut(wordList[i])
		        # str_one_hot 是 下面一个函数 word_dict_one_hot(inputs, wordDict) 的返回值
		        str_one_hot = word_dict_one_hot(tempseg, wordDict)
		        for j in range(len(str_one_hot)):
		            # 把 array 中对应的索引变成1
		            array[str_one_hot[j] - 1, i] = 1
		    pbar.finish()
		    return array
		
		
		def word_dict_one_hot(inputs, wordDict):
		    # 把 inputs 列表中的每一个字符串（比如”鲁南“， ”制药“， “公司”）对应的数值找出来，写入dict里，比如”鲁南制药公司” 是 [293, 45, 6]
		    word_one_hot = []
		    for i in range(len(inputs)):
		        # 如果这个字符串不在 wordDict 里面，那么就更新 wordDict
		        if inputs[i] not in wordDict:
		            # print(inputs[i])
		            # print("over")
		            wordDict.update({inputs[i]: len(wordDict) + 1})
		        # 如果这个字符串在 wordDict 里面，直接把对应的索引数值放进 word_one_hot 这个 list 里面
		        word_one_hot.append(wordDict[inputs[i]])
		        # print(word_one_hot)
		    return word_one_hot  # 返回值是一个dict
		
		word_index = build_word_index(wordList, wordDict)  # todo：不用pickle
		# if os.path.exists(".word_index.pkl"):
		#     print("word index exists")
		#     word_index = load_obj(".word_index")
		#     # [[1. 1. 0.... 0. 0. 0.]
		#     #  [1. 1. 0.... 0. 0. 0.]
		#     #  [1. 1. 1.... 1. 1. 0.]
		#     # ...
		#     # [0. 0. 0.... 0. 0. 0.]
		#     # [0. 0. 0.... 0. 0. 0.]
		#     # [0. 0. 0.... 0. 0. 0.]]
		# else:
		#     # 如果原本不存在 word_index，用 build_word_index(wordList, wordDict) 函数新建一个
		#     print("build word index")
		#     word_index = build_word_index(wordList, wordDict)
		    # save_obj(word_index, ".word_index")
		
		print("word index:")
		print(word_index)
		print("word index ready")
	
		print("------------------开始取词并计算相似度------------------")
	
		def get_unique(input):  # 取唯一词
		    str_unique = []
		    widgets = ['Progress get unique: ', Percentage(), ' ', Bar('#'), ' ', Timer(),
		              ' ', ETA(), ' ', FileTransferSpeed()]
		    pbar = ProgressBar(widgets=widgets, maxval=10 * len(input)).start()
		    for i in range(len(input)):
		        pbar.update(10 * i + 1)
		        time.sleep(0.0001)
		        if input[i] not in str_unique:
		            str_unique.append(input[i])
		    pbar.finish()
		    return str_unique
	
	
		# 5/输入进行搜索的词并进行预处理
		def numpy_one_hot(input1):
		    # 把 dict （ eg. [293, 45, 20, 4, 5, 6]）转换成 numpy array - 行数是maxnum，列数是len(input1)也就是字典里有几个数字
		    maxnum = max(input1)  # 选list里面数字最大值
		    array = np.zeros((maxnum, len(input1)))
		    for i in range(len(input1)):
		        if input1[i] == 293:
		            array[input1[i] - 1, i] = 3
		        else:
		            array[input1[i] - 1, i] = 1
		    return array
		    # 返回值是一个numpy array，把输入的词条中的分词对应值变成1或者3 其他的保持0 这个array里面每一列只有一个不为零
		
		
		def allign_onehot(input1, input2):
		    # input1 input2 是上一步 numpy_one_hot 出来的 numpy array
		    # 返回值是 将input1 input2 的 shape 用0扩充成同样的shape
		    # nparray.shape 是一个元组 （行数，列数）
		    length = max(input1.shape[1], input2.shape[1])  # 列数
		    height = max(input1.shape[0], input2.shape[0])  # 行数
		    # 数组拼接
		    if input1.shape[1] < length:  # 列增加
		        # 如果列数用的是 input2 的，即，在列的方向上 1 比 2 小，则扩充1
		        input1 = np.concatenate((input1, np.zeros((input1.shape[0], length - input1.shape[1]))), axis=1)
		    else:
		        # 反之，在列的方向上 2 比 1 小，扩充2
		        input2 = np.concatenate((input2, np.zeros((input2.shape[0], length - input2.shape[1]))), axis=1)
		
		    if input1.shape[0] < height:  # 行增加
		        input1 = np.concatenate((input1, np.zeros((height - input1.shape[0], input1.shape[1]))), axis=0)
		    else:
		        input2 = np.concatenate((input2, np.zeros((height - input2.shape[0], input2.shape[1]))), axis=0)
		
		    return input1, input2
		
		
		# 6/计算长度相似度和词语相似度
		def word_similar_onehot(input1, input2):
		    length = np.sum(input1) + np.sum(input2)  # 计算两个文本的长度
		    print(length)
		    input1 = np.sum(input1, axis=1)  # 对文本1进行维度压缩，得到该文本的词向量
		    print(input1)
		    input2 = np.sum(input2, axis=1)  # 对文本2进行维度压缩，得到该文本的词向量
		    print(input2)
		    similar = 1 - np.sum(np.abs((input1 - input2)) / length)  # 计算词语相似度 1-3/13
		    # 数值越大（越接近1）相似度越高
		    return similar
		
		
		def order_similar_onehot(input1, input2):
		    similar = np.sum(np.abs(input1 - input2))  # 计算顺序相似度
		    print(np.abs(input1 - input2))
		    # 括号里面的是一个 nparray，表示 1 和 2 之间每一个值的差（0 or 1）
		    # 然后把这个差值加和得到顺序相似度
		    # 数值越小相似度越高
		    return similar
	
	
		print("-" * 100)
		str_one_hot1 = word_dict_one_hot(get_unique(seg.cut(in_word1.str)), wordDict)
		str_one_hot2 = word_dict_one_hot(get_unique(seg.cut(in_word2.str)), wordDict)
		print(str_one_hot1)
		print(str_one_hot2)
		
		print("-" * 100)
		str_one_hot1 = numpy_one_hot(str_one_hot1)
		str_one_hot2 = numpy_one_hot(str_one_hot2)
		print("np数组：")
		print(str_one_hot1)
		print(str_one_hot1.shape)
		print(str_one_hot2)
		print(str_one_hot2.shape)
		
		print("-" * 100)
		str_one_hot1, str_one_hot2 = allign_onehot(str_one_hot1, str_one_hot2)
		print("数组拼接：")
		print(str_one_hot1)
		print(str_one_hot1.shape)
		print(str_one_hot2)
		print(str_one_hot2.shape)
		
		cs = word_similar_onehot(str_one_hot1, str_one_hot2)
		ors = order_similar_onehot(str_one_hot1, str_one_hot2)
		
		print("-" * 100)
		print(cs)
		print(ors)
		
		return cs
	
	# cscs = one_hot("鲁南制药公司", "鲁南制药集团股份有限公司")
	# print(cscs)
	# @func.udf(returnType=IntegerType())
	# @pandas_udf(functionType=PandasUDFType.GROUPED_MAP)
	def test(i, j):
		return i+j
		
	test_pandas = pandas_udf(one_hot, returnType=FloatType())
	
	cpa_prod_join_data.withColumn("ed_MNF_NAME_CH", (test_pandas(col("in_MANUFACTURER_NAME"), col("check_MNF_NAME_CH")))).show()
	
	# cpa_prod_join_data = cpa_prod_join_data.withColumn("ed_MNF_NAME_CH", test("in_MANUFACTURER_NAME", "check_MNF_NAME_CH"))
	# cpa_prod_join_data.show(4)
	# print(cpa_prod_join_data.count())  # 37173


	
	
	
	# out_path = out_path + "/" + "cpa_one_hot"
	# cpa_one_hot.write.format("parquet").mode("overwrite").save(out_path)
	# print("写入 " + out_path + " 完成")


	print("程序end job4_1_one_hot")
	print("--"*80)