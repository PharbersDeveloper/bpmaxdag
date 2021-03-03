# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This is job template for Pharbers Max Job
"""
from phcli.ph_logs.ph_logs import phs3logger
import uuid
import re
from functools import reduce
import numpy as np
import pandas as pd
from pyspark.sql.functions import col , concat_ws
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
from pyspark.sql.functions import split , count , when , lit
from pyspark.sql.functions import regexp_replace, upper, regexp_extract
from pyspark.ml.feature import StopWordsRemover


def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"])
    spark = kwargs["spark"]()
    logger.info(kwargs)

###############----------input--------------------################
    path_master_prod = kwargs["path_master_prod"]
    path_standard_gross_unit = kwargs["path_standard_gross_unit"]
    path_for_replace_standard_dosage = kwargs["path_for_replace_standard_dosage"]
###############----------input--------------------################

###############----------output-------------------################
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["standard_result"]
    origin_path = result_path_prefix + kwargs["standard_origin"]
###############----------output--------------------################

###########--------------load file----------------------- ################
    df_standard = load_standard_prod(spark, path_master_prod)
    df_standard.write.mode("overwrite").parquet(origin_path)
    df_standard_gross_unit  = load_standard_gross_unit(spark,path_standard_gross_unit)
    df_replace_standard_dosage  = load_replace_standard_dosage(spark,path_for_replace_standard_dosage) 
###########--------------load file----------------------- ################


#########--------------main function--------------------################# 

    #DOSAGE预处理
    df_standard = make_dosage_standardization(df_standard,df_replace_standard_dosage)

    
##################------------新计算方法----------------####################
    #spec转成结构化数据
    df_standard = make_spec_become_structured(df_standard)
    #词形还原
    df_standard = restore_nonstandard_data_to_normal(df_standard)
    ''' 
    #基于不同的总量单位进行SPEC数据提取
    df_standard = extract_useful_cpa_spec_data(df_standard)
    #单位归一化处理
    df_standard = make_unit_standardization(df_standard)
    #组合成新SPEC
    df_standard = create_new_cpa_spec_col(df_standard)
    #从cpa spec中抽取pack_id
    df_standard = get_cpa_pack(df_standard)
    df_standard = get_pca_inter(df_standard,df_second_interfere)
    #选择指定的列
    df_standard = select_cpa_col(df_standard)
##################------------新计算方法----------------####################
    #添加标准总量单位
	df_standard = add_standard_gross_unit(df_standard,df_standard_gross_unit)
    #SPEC数据预处理
	df_standard = pre_to_standardize_data(df_standard)
    #基于不同的总量单位进行SPEC数据提取
	df_standard = extract_useful_spec_data(df_standard)
    #数据提纯
	df_standard = make_spec_gross_and_valid_pure(df_standard)
    #单位归一化处理
	df_standard = make_spec_unit_standardization(df_standard)
   #组合成新SPEC
	df_standard = create_new_spec_col(df_standard)
	df_standard.write.mode("overwrite").parquet(result_path)
#########--------------main function--------------------################# 
    '''
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


"""
读取标准表WW
"""
def load_standard_prod(spark, standard_prod_path):
    df_standard = spark.read.parquet(standard_prod_path) \
                        .select("PACK_ID",
                                "MOLE_NAME_CH", "MOLE_NAME_EN",
                                "PROD_DESC", "PROD_NAME_CH",
                                "CORP_NAME_EN", "CORP_NAME_CH", "MNF_NAME_EN", "MNF_NAME_CH",
                                "PCK_DESC", "DOSAGE", "SPEC", "PACK")

    df_standard = df_standard.withColumnRenamed("PACK_ID", "PACK_ID_STANDARD") \
                            .withColumnRenamed("MOLE_NAME_CH", "MOLE_NAME_STANDARD") \
                            .withColumnRenamed("PROD_NAME_CH", "PRODUCT_NAME_STANDARD") \
                            .withColumnRenamed("CORP_NAME_CH", "CORP_NAME_STANDARD") \
                            .withColumnRenamed("MNF_NAME_CH", "MANUFACTURER_NAME_STANDARD") \
                            .withColumnRenamed("MNF_NAME_EN", "MANUFACTURER_NAME_EN_STANDARD") \
                            .withColumnRenamed("DOSAGE", "DOSAGE_STANDARD") \
                            .withColumnRenamed("SPEC", "SPEC_STANDARD") \
                            .withColumnRenamed("PACK", "PACK_QTY_STANDARD")
                            
    df_standard = df_standard.select("PACK_ID_STANDARD", "MOLE_NAME_STANDARD",
                                     "PRODUCT_NAME_STANDARD", "CORP_NAME_STANDARD",
                                     "MANUFACTURER_NAME_STANDARD", "MANUFACTURER_NAME_EN_STANDARD",
                                     "DOSAGE_STANDARD", "SPEC_STANDARD", "PACK_QTY_STANDARD")

    return df_standard

def load_standard_gross_unit(spark,path_standard_gross_unit):
    df_standard_gross_unit = spark.read.parquet(path_standard_gross_unit)
    return df_standard_gross_unit
    
def load_replace_standard_dosage(spark,path_for_replace_standard_dosage):
    df_replace_standard_dosage = spark.read.parquet(path_for_replace_standard_dosage)
    return df_replace_standard_dosage
    
def make_dosage_standardization(df_standard,df_replace_standard_dosage):
    #标准表DOSAGE中干扰项剔除
    replace_dosage_str = r'(([(（].*[)）])|(\s+))'
    df_standard = df_standard.withColumn("DOSAGE_STANDARD", regexp_replace(col("DOSAGE_STANDARD"),replace_dosage_str,""))\
    .dropna(subset="DOSAGE_STANDARD")
    df_standard = df_standard.withColumn("DOSAGE_STANDARD", when(col("DOSAGE_STANDARD") == "鼻喷剂","鼻用喷雾剂")\
                                         .when(col("DOSAGE_STANDARD") == "胶囊","胶囊剂")\
                                         .when(col("DOSAGE_STANDARD") == "阴道洗剂","洗剂")\
                                         .when(col("DOSAGE_STANDARD") == "混悬剂","干混悬剂")\
                                         .when(col("DOSAGE_STANDARD") == "颗粒","颗粒剂")\
                                         .when(col("DOSAGE_STANDARD") == "糖浆","糖浆剂")\
                                         .when(col("DOSAGE_STANDARD") == "泡腾颗粒","泡腾颗粒剂")\
                                         .otherwise(col("DOSAGE_STANDARD")))  

    return df_standard
def make_spec_become_structured(df_standard):
    df_standard = df_standard.withColumn('SPEC_STANDARD_ORIGINAL', col("SPEC_STANDARD"))
    split_spec_str = r'(\s+)'
    df_standard = df_standard.withColumn("SPEC_STANDARD", split(col("SPEC_STANDARD"), split_spec_str,).cast(ArrayType(StringType())))
    stopwords = ['PAED','OTCP','SUFR','ADLT','PAP.','IRBX','','/DOS','/ML','TN','x','INF.','/G']
    remover = StopWordsRemover(stopWords=stopwords, inputCol="SPEC_STANDARD", outputCol="SPEC__STANDARD_TEMP")
    df_standard = remover.transform(df_standard)
    df_standard = df_standard.drop("SPEC_STANDARD").withColumnRenamed("SPEC__STANDARD_TEMP","SPEC_STANDARD")
    return df_standard

#拆分数字及单位
@pandas_udf(ArrayType(StringType()),PandasUDFType.SCALAR)
def make_single_word_to_double(cpa_valid_data):
    frame = {'cpa_valid_data':cpa_valid_data}
    df = pd.DataFrame(frame)
    def remove_stopwords(df):
        if  len(df.cpa_valid_data) ==1  and df.cpa_valid_data[-1].isdigit() == False  and df.cpa_valid_data[-1].isalpha() == False: 
            try:
                integer_split_pattern = r'^(\d+)(\w+)$' 
                df['cpa_valid_data'] = np.array(re.match(integer_split_pattern, str(df.cpa_valid_data[-1])).groups([0,-1]))
            except:
                df['cpa_valid_data'] = np.array(df.cpa_valid_data)
        else: 
            df['cpa_valid_data'] = np.array(df.cpa_valid_data) 
        if len(df.cpa_valid_data) ==1 and df.cpa_valid_data[-1].isdigit() == False  and df.cpa_valid_data[-1].isalpha() == False: 
            try:
                decimal_split_pattern = r'^(\d+\.\d+)(\w+)$' 
                df['cpa_valid_data'] = np.array(re.match(decimal_split_pattern, str(df.cpa_valid_data[-1])).groups([0,-1]))
            except:
                df['cpa_valid_data'] = np.array(df.cpa_valid_data)
        else: 
            df['cpa_valid_data'] = np.array(df.cpa_valid_data)     
        return df['cpa_valid_data']

    df['SPEC_CPA_VALID_DATA'] = df.apply(remove_stopwords, axis=1)
    return df['SPEC_CPA_VALID_DATA'] 

def extract_useful_cpa_spec_data(df_standard):
    
    df_standard = df_standard.withColumn("SPEC_STANDARD_GROSS_DATA", extract_gross_data(col("SPEC_STANDARD")))
    df_standard = df_standard.withColumn("SPEC_STANDARD_VALID_DATA", extract_valid_data(col("SPEC_STANDARD")))
    return df_standard

#总量数据的提取
@pandas_udf(ArrayType(StringType()),PandasUDFType.SCALAR)
def extract_gross_data(spec):
    frame = {
       "spec":spec
   } 
    df = pd.DataFrame(frame)
    def add_cpa_gross_data(df):
        if len(df.spec) >= 2 :
            if df.spec[-1].isdigit() == False  and df.spec[-1].isalpha() == False:
                df["STANDARD_GROSS_DATA"] = np.array(['{}'.format(df.spec[-1])]) 
            else:
                df["STANDARD_GROSS_DATA"] = np.array(df.spec[-2:])
        else:
            df["STANDARD_GROSS_DATA"] = np.array(df.spec)
        return df["STANDARD_GROSS_DATA"]
    df["STANDARD_GROSS_DATA"] = df.apply(add_cpa_gross_data, axis=1)
    return df["STANDARD_GROSS_DATA"]

#有效性数据提取
@pandas_udf(ArrayType(StringType()),PandasUDFType.SCALAR)
def extract_valid_data(spec):
    frame = {
        "spec":spec,
    }
    df = pd.DataFrame(frame)

    def add_valid_data(df):
        if len(df.spec) == 1:
            df["STANDARD_VALID_DATA"] = np.array([]) 
        else:
            df["STANDARD_VALID_DATA"] =  np.array(df.spec[:-1])
        return df["STANDARD_VALID_DATA"]

    df["STANDARD_VALID_DATA"] = df.apply(add_valid_data, axis=1) 
    return df["STANDARD_VALID_DATA"]

#处理spec中非标准数据
def restore_nonstandard_data_to_normal(df_standard):
    df_standard = df_standard.withColumn("SPEC_STANDARD_TEMP",make_nonstandard_data_become_normal(col("SPEC_STANDARD")))
    df_standard.select("SPEC_STANDARD_ORIGINAL","SPEC_STANDARD","SPEC_STANDARD_TEMP").distinct().show(200)
    print(df_standard.printSchema()) 
    return df_standard

@pandas_udf(ArrayType(StringType()), PandasUDFType.SCALAR)
def make_nonstandard_data_become_normal(origin_col):
    def judge_nonstandard_data(word):
        data_extraction_rule = r'(\d+(\.\d+)?)(\w+(?=\+))'
        if re.match(data_extraction_rule, word) != None:
            the_first_data = re.findall(data_extraction_rule, word)[0]
            the_first_data_value = the_first_data[0]
            the_first_data_unit = the_first_data[-1]
            the_test_of_data_extract_rule = r'\+(\d+(\.\d+)?)(\w+)' 
            the_rest_of_data = re.findall(the_test_of_data_extract_rule, word)
            the_rest_of_data_values = list(map(lambda x : x[0], the_rest_of_data))
            the_rest_of_data_units = list(map(lambda x: x[-1], the_rest_of_data))
            if len(set(the_rest_of_data_units)) == 1 and the_first_data_unit in the_rest_of_data_units:
                try:
                    the_sum_of_rest_data_values = reduce(lambda x , y: float(x) + float(y), the_rest_of_data_values)
                    the_sum_of_all_data = float(the_first_data_value) + float(the_sum_of_rest_data_values)
                except:
                    the_sum_of_all_data = float(the_first_data_value)
                final_data = str(the_sum_of_all_data) + the_first_data_unit
            else:
                final_data = word
        else:
            final_data = word
        return final_data 
    frame = {"origin_col": origin_col}
    df = pd.DataFrame(frame) 
    df['out_put_col'] = df.apply(lambda x : np.array(tuple(map(judge_nonstandard_data, x.origin_col))), axis=1)
    return df['out_put_col']

def make_unit_standardization(df_standard):
    
    df_standard = df_standard.withColumn("SPEC_STANDARD_GROSS_DATA", create_values_and_units(col("SPEC_STANDARD_GROSS_DATA")))
    df_standard = df_standard.withColumn("SPEC_STANDARD_VALID_DATA", create_values_and_units(col("SPEC_STANDARD_VALID_DATA")))
    ''' 
    df_standard = df_standard.withColumn("SPEC_STANDARD_GROSS_DATA", make_spec_units_normal(col("SPEC_STANDARD_GROSS_DATA")))
    df_standard = df_standard.withColumn("SPEC_STANDARD_VALID_DATA", make_spec_units_normal(col("SPEC_STANDARD_VALID_DATA")))
    df_standard = df_standard.withColumn("SPEC_STANDARD_GROSS_DATA", combine_values_and_units(col("SPEC_STANDARD_GROSS_DATA")))
    df_standard = df_standard.withColumn("SPEC_STANDARD_VALID_DATA", combine_values_and_units(col("SPEC_STANDARD_VALID_DATA")))
    '''
    return df_standard 

@pandas_udf(ArrayType(StringType()), PandasUDFType.SCALAR)
def create_values_and_units(origin_col):
    unit_data_dict = {
        'G':'MG',
        'UG':'MG',
        'Y':'MG',
        'GM':'MG',
        'L':'ML',
        'IU':'U',
        'MU':'U',
        'MIU':'U',
        'k':'U',
        'AXAIU':'U',
        'mCi':'MC',
        'MG':'MG'
    }
    conversion_dict ={
        'G':1000,
        'UG':0.001,
        'Y':0.001,
        'GM':1,
        'L':1000,
        'MU':1000000,
        'M':100000,
        'MIU':1000000,
        'K':1000,
        'MG':1
    }

        #分割字符
    def split_int_word(word):
        
        pattern_decimal = r'(\d+\.\d+)(\w+)$'
        try:
            if re.match(pattern_decimal, word) == None:
                pattern_decimal = r'(\d+)(\w+)$'
            s = re.findall(pattern_decimal,word)[0]
            value = str(float(s[0]) * float(conversion_dict[s[1]]))
            unit = str(unit_data_dict[s[1]])
            temp_data = value + unit
        except:
            temp_data = word
        return temp_data 
   
    frame = {"origin_col":origin_col}
    df = pd.DataFrame(frame)
    df['out_put_col'] = df.apply(lambda x: np.array(tuple(map(split_int_word, x.origin_col))),axis=1)
    
    return df['out_put_col']

#单位统一
@pandas_udf(ArrayType(StringType()), PandasUDFType.SCALAR)
def make_spec_units_normal(original_col):
    frame = {"original_col":original_col}  
    df = pd.DataFrame(frame)
    unit_data_dict = {
        'G':'MG',
        'UG':'MG',
        'Y':'MG',
        'GM':'MG',
        'L':'ML',
        'IU':'U',
        'MU':'U',
        'MIU':'U',
        'k':'U',
        'AXAIU':'U',
        'mCi':'MC',
        'MG':'MG'
    }
    conversion_dict ={
        'G':1000,
        'UG':0.001,
        'Y':0.001,
        'GM':1,
        'L':1000,
        'MU':1000000,
        'M':100000,
        'MIU':1000000,
        'K':1000,
        'MG':1
    }
    def integrate_spec_data(df):
        nonlocal unit_data_dict
        nonlocal conversion_dict
        try:
            if len(df.original_col) == 2 :
                
                df.original_col[0] = str(float(df.original_col[0]) * float(conversion_dict[df.original_col[-1]]))
                df.original_col[-1] = str(unit_data_dict[df.original_col[-1]])
                df['original_col'] = np.array(df.original_col)
            else:
                df['original_col'] = np.array(df.original_col)
        except:
            df['original_col'] = np.array(df.original_col)
        return df['original_col']
    df["original_col"] =  df.apply(integrate_spec_data, axis=1) 
    return df["original_col"] 

#合并数值和单位
@pandas_udf(StringType(), PandasUDFType.SCALAR)
def combine_values_and_units(original_col):
    frame = {"original_col":original_col}
    df = pd.DataFrame(frame)
    def judge_values_and_units(df):
        if len(df.original_col) == 2 and  df.original_col[-1].isalpha() == True:
            df['out_put_col'] = ''.join(df.original_col)
        elif len(df.original_col) == 0:
            df['out_put_col'] = ''
        elif len(df.original_col) == 3 and df.original_col[-1].isalpha() == True:
            df['out_put_col'] = df.original_col[0] + ' ' + ''.join(df.original_col[1,2])
        else:
            df['out_put_col'] = ''.join(df.original_col)
        return df['out_put_col'] 
    df['out_put_col'] = df.apply(judge_values_and_units, axis=1) 
    return df['out_put_col']

############################-----新计算方法---------####################################################    
def add_standard_gross_unit(df_standard,df_standard_gross_unit):
    
	df_standard_gross_unit_mg = df_standard_gross_unit.select('STANDARD_GROSS_UNIT_MG')\
													.withColumnRenamed('STANDARD_GROSS_UNIT_MG','DOSAGE_STANDARD')\
													.withColumn('STANDARD_GROSS_UNIT',lit('MG'))
	df_standard_gross_unit_ml = df_standard_gross_unit.select('STANDARD_GROSS_UNIT_ML')\
														.withColumnRenamed('STANDARD_GROSS_UNIT_ML','DOSAGE_STANDARD')\
														.withColumn('STANDARD_GROSS_UNIT',lit('ML'))
	df_standard_gross_unit_cm = df_standard_gross_unit.select('STANDARD_GROSS_UNIT_CM')\
													.withColumnRenamed('STANDARD_GROSS_UNIT_CM','DOSAGE_STANDARD')\
													.withColumn('STANDARD_GROSS_UNIT',lit('CM'))
	df_standard_gross_unit_pen = df_standard_gross_unit.select('STANDARD_GROSS_UNIT_PEN')\
														.withColumnRenamed('STANDARD_GROSS_UNIT_PEN','DOSAGE_STANDARD')\
														.withColumn('STANDARD_GROSS_UNIT',lit('喷'))
	df_standard_gross_unit_mm = df_standard_gross_unit.select('STANDARD_GROSS_UNIT_MM')\
													.withColumnRenamed('STANDARD_GROSS_UNIT_MM','DOSAGE_STANDARD')\
													.withColumn('STANDARD_GROSS_UNIT',lit('MM'))
	df_standard_gross_unit = df_standard_gross_unit_mg\
							.union(df_standard_gross_unit_ml)\
							.union(df_standard_gross_unit_cm)\
							.union(df_standard_gross_unit_pen)\
							.union(df_standard_gross_unit_mm)\
							.filter(col('DOSAGE_STANDARD').isNotNull())
	df_standard = df_standard.join(df_standard_gross_unit,df_standard.DOSAGE_STANDARD == df_standard_gross_unit.DOSAGE_STANDARD , 'left').drop(df_standard_gross_unit.DOSAGE_STANDARD)
	return df_standard

def pre_to_standardize_data(df_standard):
    #剔除SPEC中空格
	remove_spaces_spec = r'(\s+)'
	df_standard = df_standard.withColumn("SPEC_STANDARD", regexp_replace(col("SPEC_STANDARD"), remove_spaces_spec , ""))\
							.withColumn("SPEC_STANDARD", regexp_replace(col("SPEC_STANDARD"), r"(/DOS)" , "喷"))\
							.withColumn("SPEC_STANDARD", upper(col("SPEC_STANDARD")))
	return df_standard

def extract_useful_spec_data(df_standard):
    #总量数据的提取
	extract_spec_value_MG = r'(\d+\.?\d*(((GM)|(MU)|[MU]?G)|Y|(ΜG)|(PNA)))'
	extract_spec_value_ML = r'(\d+\.?\d*((M?L)|(PE)))'
	extract_spec_value_U = r'(\d+\.?\d*((I?U)|(TIU)))'
	extract_spec_value_PEN = r'(喷(\d+\.?\d*))'
	extract_spec_value_CM = r'(\d+\.?\d*(CM)?[×:*](\d+\.?\d*(CM)?)([×*](\d+\.?\d*(CM)?))?|(\d+\.?\d*(CM)))'
	extract_pure_spec_valid_value = r'(喷?(\d+\.?\d*)((M?L)|([MU]?G)|I?[U喷KY]|(C?M)))'
	df_standard = df_standard.withColumn('STANDARD_SPEC_GROSS_VALUE', when(col('STANDARD_GROSS_UNIT') == 'MG' , regexp_extract(col('SPEC_STANDARD'), extract_spec_value_MG, 1))\
															.when(col('STANDARD_GROSS_UNIT') == 'ML' , regexp_extract(col('SPEC_STANDARD'), extract_spec_value_ML, 1))\
															.when(col('STANDARD_GROSS_UNIT') == 'U' , regexp_extract(col('SPEC_STANDARD'), extract_spec_value_U, 1))\
															.when(col("STANDARD_GROSS_UNIT") == "喷" , regexp_extract(col('SPEC_STANDARD'), extract_spec_value_PEN, 1))\
															.when(col('STANDARD_GROSS_UNIT') == 'CM' , regexp_extract(col('SPEC_STANDARD'), extract_spec_value_CM, 1)))\
															.withColumn('STANDARD_SPEC_GROSS_VALUE', when( col('STANDARD_SPEC_GROSS_VALUE') == '', regexp_extract(col('SPEC_STANDARD'), extract_pure_spec_valid_value, 1))\
															.otherwise(col('STANDARD_SPEC_GROSS_VALUE')))
	print(  '标准表数据总数：' + str(df_standard.count()))
	print('匹配失败数据：' + ' ' + str(df_standard.filter(col('STANDARD_SPEC_GROSS_VALUE') == '').count()) ,  '匹配率:' +' ' +  str(( 1 - int(df_standard.filter(col('STANDARD_SPEC_GROSS_VALUE') == '').count()) / int(df_standard.count())) * 100) + '%' ) 
	df_standard.filter(col('STANDARD_SPEC_GROSS_VALUE') == '').groupBy("SPEC_STANDARD").agg(count(col("SPEC_STANDARD"))).show(200)
    
	return df_standard

def make_spec_gross_and_valid_pure(df_standard):
    
    #数据提纯
	extract_pure_spec_valid_value = r'(\d+\.?\d*)((MU)|(M?L)|([MU]?G)|I?[U喷KY]|(C?M))'
	df_standard = df_standard.withColumn("SPEC_GROSS_VALUE_PURE", regexp_extract(col("STANDARD_SPEC_GROSS_VALUE"),extract_pure_spec_valid_value,1))\
								.withColumn("SPEC_GROSS_UNIT_PURE", regexp_extract(col("STANDARD_SPEC_GROSS_VALUE"),extract_pure_spec_valid_value,2))
	return df_standard

def make_spec_unit_standardization(df_standard):

	df_standard = df_standard.withColumn("SPEC_GROSS_VALUE_PURE", col("SPEC_GROSS_VALUE_PURE").cast("double"))\
								.withColumn("SPEC_valid_digit_STANDARD", col("SPEC_valid_digit_STANDARD").cast("double"))
#总量数值归一化
	df_standard = df_standard.withColumn("SPEC_GROSS_VALUE_PURE",when(col("SPEC_GROSS_UNIT_PURE") == "G", col("SPEC_GROSS_VALUE_PURE")*int(1000))\
								.when(col("SPEC_GROSS_UNIT_PURE") == "UG", col("SPEC_GROSS_VALUE_PURE")*int(0.001))\
								.when(col("SPEC_GROSS_UNIT_PURE") == "Y", col("SPEC_GROSS_VALUE_PURE")*int(0.001))\
								.when(col("SPEC_GROSS_UNIT_PURE") == "L", col("SPEC_GROSS_VALUE_PURE")*int(1000))\
								.when(col("SPEC_GROSS_UNIT_PURE") == "MU", col("SPEC_GROSS_VALUE_PURE")*int(1000000))\
								.when(col("SPEC_GROSS_UNIT_PURE") == "MIU", col("SPEC_GROSS_VALUE_PURE")*int(1000000))\
								.when(col("SPEC_GROSS_UNIT_PURE") == "K", col("SPEC_GROSS_VALUE_PURE")*int(1000))\
								.otherwise(col("SPEC_GROSS_VALUE_PURE")))
	df_standard = df_standard.withColumn("SPEC_GROSS_VALUE_PURE" , when(col("SPEC_GROSS_VALUE_PURE").isNull(),regexp_extract(col("SPEC_STANDARD"),r"喷(\d+\.?\d*)",1))\
								.otherwise(col("SPEC_GROSS_VALUE_PURE")))\
								.withColumn("SPEC_GROSS_VALUE_PURE", col("SPEC_GROSS_VALUE_PURE").cast("double"))
#删除辅助列print(df_standard.columns)
	df_standard =df_standard.withColumnRenamed("SPEC_STANDARD","SPEC_ORIGIN")\
							.withColumnRenamed("SPEC_GROSS_VALUE_PURE","SPEC_GROSS_VALUE_PURE_STANDARD")\
							.withColumnRenamed("STANDARD_GROSS_UNIT","SPEC_GROSS_UNIT_PURE_STANDARD")\
							.drop("SPEC_gross_digit_STANDARD","SPEC_gross_unit_STANDARD","SPEC_gross_unit_STANDARD","SPEC_GROSS_UNIT_PURE","STANDARD_SPEC_GROSS_VALUE")

	return df_standard


def create_new_spec_col(df_standard):
    
	df_standard = df_standard.withColumn("SPEC_STANDARD",concat_ws('/',col("SPEC_valid_digit_STANDARD"),col("SPEC_valid_unit_STANDARD"),col("SPEC_GROSS_VALUE_PURE_STANDARD"),col("SPEC_GROSS_UNIT_PURE_STANDARD")))
	col_list = ['MOLE_NAME_STANDARD', 'PRODUCT_NAME_STANDARD', 'DOSAGE_STANDARD', 'SPEC_STANDARD', 'MANUFACTURER_NAME_STANDARD','MANUFACTURER_NAME_EN_STANDARD', 'CORP_NAME_STANDARD','PACK_QTY_STANDARD', 'PACK_ID_STANDARD', 'SPEC_valid_digit_STANDARD', 'SPEC_valid_unit_STANDARD', 'SPEC_GROSS_VALUE_PURE_STANDARD','SPEC_GROSS_UNIT_PURE_STANDARD']

	df_standard = df_standard.select(col_list) 
	df_standard.select("SPEC_STANDARD").show(500)
	return df_standard


################-----------------------functions---------------------------################