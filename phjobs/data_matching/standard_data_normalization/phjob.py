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
    write_files(input_dataframe=df_standard,\
                path_of_write=origin_path,\
                file_type="parquet",\
                repartition_num=10)
###########--------------load file----------------------- ################


#########--------------main function--------------------################# 

    #dosage处理
    df_standard = make_dosage_standardization(df_standard)

    
    #spec转成结构化数据
    df_standard = make_spec_become_structured(df_standard)
    
    #词形还原
    df_standard = restore_nonstandard_data_to_normal(df_standard)
    #数据单位标准化
    df_standard = make_unit_standardization(df_standard)
    #spec有效性和总量拆分
    df_standard = extract_spec_valid_and_gross(df_standard)
    #spec array转string类型
    df_standard = make_spec_become_string(df_standard)
    
    write_files(input_dataframe=df_standard,\
                path_of_write=result_path,\
                file_type="parquet",\
                repartition_num=10)
    
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

def make_dosage_standardization(df_standard):
    #标准表DOSAGE中干扰项剔除
    replace_dosage_str = r'(([(（].*[)）])|(\s+))'
    df_standard = df_standard.withColumn("DOSAGE_STANDARD", regexp_replace(col("DOSAGE_STANDARD"),replace_dosage_str,""))\
    .dropna(subset="DOSAGE_STANDARD")
    
    return df_standard

#spec数据改为array
def make_spec_become_structured(df_standard):
    df_standard = df_standard.withColumn('SPEC_STANDARD_ORIGINAL', col("SPEC_STANDARD"))
    split_spec_str = r'(\s+)'
    df_standard = df_standard.withColumn("SPEC_STANDARD", split(col("SPEC_STANDARD"), split_spec_str,).cast(ArrayType(StringType())))
    stopwords = ['PAED','OTCP','SUFR','ADLT','PAP.','IRBX','','/DOS','/ML','TN','x','INF.','/G','FSH','LH']
    remover = StopWordsRemover(stopWords=stopwords, inputCol="SPEC_STANDARD", outputCol="SPEC__STANDARD_TEMP")
    df_standard = remover.transform(df_standard)
    df_standard = df_standard.drop("SPEC_STANDARD").withColumnRenamed("SPEC__STANDARD_TEMP","SPEC_STANDARD")
    return df_standard

def extract_useful_cpa_spec_data(df_standard):
    
    df_standard = df_standard.withColumn("SPEC_STANDARD_GROSS_DATA", extract_gross_data(col("SPEC_STANDARD")))
    df_standard = df_standard.withColumn("SPEC_STANDARD_VALID_DATA", extract_valid_data(col("SPEC_STANDARD")))
    return df_standard

#处理spec中非标准数据
def restore_nonstandard_data_to_normal(df_standard):
    df_standard = df_standard.withColumn("SPEC_STANDARD", make_nonstandard_data_become_normal_addType(col("SPEC_STANDARD")))
    df_standard = df_standard.withColumn("SPEC_STANDARD", make_nonstandard_data_become_normal_percent_or_rateType(col("SPEC_STANDARD")))

    return df_standard

#处理spec中add类型数据
@pandas_udf(ArrayType(StringType()), PandasUDFType.SCALAR)
def make_nonstandard_data_become_normal_addType(origin_col):
    def judge_nonstandard_data(word):
        data_extraction_rule = r'(\d+(\.\d+)?)(\w+(?=\+))'
        if len(re.findall(data_extraction_rule, word)) != 0:
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
    df['out_put_col'] = df.apply(lambda x:np.array(tuple(map(judge_nonstandard_data, x.origin_col))), axis=1)
    return df['out_put_col']

#处理spec中浓度
@pandas_udf(ArrayType(StringType()), PandasUDFType.SCALAR)
def make_nonstandard_data_become_normal_percent_or_rateType(origin_col):
    frame = {"origin_col": origin_col}
    df = pd.DataFrame(frame) 
    def make_elements_of_list_into_one_string(origin_list):
        placeholder_word = ' '
        output_sentence = reduce(lambda x,y: x + f"{placeholder_word}" + y ,origin_list)
        return output_sentence
    def extract_gross_data(sentence):
        percent_extract_pattern = r'\d+(\.\d+)?(?=.*?)%'
        remove_placeholder = ''
        if len(re.findall(percent_extract_pattern, sentence)) != 0:
            sentence = re.sub(percent_extract_pattern,remove_placeholder,sentence)
        gross_data_pattern = r'[\+]?(\d+(\.\d+)?)(?!\d+)(\w+)'
        try:
            if len(re.findall(gross_data_pattern, sentence)) == 0:
                gross_data = sentence
            else:
                extract_data_list = re.findall(gross_data_pattern, sentence)
                max_gross_value_units = list(map(lambda x: x[-1], extract_data_list))
                gross_value_list = list(map(lambda x: float(x[0]), extract_data_list))
#                 max_gross_value = max(gross_value_list)
                max_gross_value = gross_value_list[-1]            #单位的优先级还没做，先暂时选最后一位数据作为总量
                max_gross_value_index = gross_value_list.index(max_gross_value)
                max_gross_value_unit = max_gross_value_units[max_gross_value_index]
                gross_data = str(max_gross_value) + max_gross_value_unit
        except:
            gross_data = sentence
        return gross_data
    def extract_value_and_unit(word):
        gross_data_pattern = r'[\+]?(\d+(\.\d+)?)(\w+)'
        try:
            if len(re.findall(gross_data_pattern,word)) == 0:
                return None
            else:
                gross_data = re.findall(gross_data_pattern,word)[0]
                gross_value = gross_data[0]
                gross_unit = gross_data[-1]
                return (gross_value,gross_unit)
        except:
            return None
    def make_percent_or_rate_into_normal(word,gross_data):
        gross_data = extract_value_and_unit(gross_data)
        if gross_data == None:
            data = word
        else:
            rate_pattern = r'[\s+()]?(\d+)[：:](\d+)'
            percent_pattern = r'[\s+]?(\d+(\.\d+)?)(?=%)'
            gross_value = gross_data[0]
            gross_unit = gross_data[-1]
            if len(re.findall(percent_pattern, word)) != 0:
                match_percent_data = re.findall(percent_pattern, word)[0]
                if len(match_percent_data) >= 1:
                    percent_data = match_percent_data[0]
                    numerical_value = float(float(percent_data) / 100)
                    if gross_unit == "ML":
                        gross_unit = "MG"
                        numerical_value = numerical_value * 1000
                    data = str(float(numerical_value * float(gross_value))) + gross_unit
                else:
                    data = word
            elif len(re.findall(rate_pattern, word)) != 0:
                match_rate_data = re.findall(rate_pattern, word)[0]
                if len(match_rate_data) >=1 :
                    min_value = min(match_rate_data)
                    max_value = max(match_rate_data)
                    numerical_value = float( int(min_value) / (int(min_value) + int(max_value)))
                    if gross_unit == "ML":
                        gross_unit = "MG"
                        numerical_value = numerical_value * 1000
                    data = str(float(numerical_value * float(gross_value))) + gross_unit
                else:
                    data = word
            else:    
                
                    data = word
        return data
    
    def execute_percent_or_rate_into_normal(origin_list):
        sentence = make_elements_of_list_into_one_string(origin_list)
        gross_data = extract_gross_data(sentence)
        out_put_file = list(map(lambda x: make_percent_or_rate_into_normal(x,gross_data),origin_list))
        return out_put_file
    
    df['out_put_col'] = df.apply( lambda x: np.array(execute_percent_or_rate_into_normal(x.origin_col)), axis=1)
    return df['out_put_col']

#数据单位标准化
def make_unit_standardization(df_standard):
    
    df_standard = df_standard.withColumn("SPEC_STANDARD", create_values_and_units(col("SPEC_STANDARD")))

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
#         extract_pattern = r'[\+]?(\d+(\.\d+)?)([^%]\w+)'
#         try:
#             if len(re.findall(gross_data_pattern,word)) == 0:
#                 return word
#             else:
#                 extract_data = re.findall(gross_data_pattern,word)[0]
#                 extract_value = gross_data[0]
#                 extract_unit = gross_data[-1]
#                 value = str(float(extract_value) * float(conversion_dict[extract_unit]))
#                 word = value + extract_unit
#                 return word
#         except:
#                 return word

    frame = {"origin_col":origin_col}
    df = pd.DataFrame(frame)
    df['out_put_col'] = df.apply(lambda x: np.array(tuple(map(split_int_word, x.origin_col))),axis=1)
    return df['out_put_col']


def extract_spec_valid_and_gross(df_standard):
    df_standard = df_standard.withColumn("SPEC_STANDARD_GROSS", make_spec_gross_data(col("SPEC_STANDARD")))
    df_standard = df_standard.withColumn("SPEC_STANDARD_VALID", make_spec_valid_data(col("SPEC_STANDARD"),col("SPEC_STANDARD_GROSS")))
    return df_standard

@pandas_udf(StringType(), PandasUDFType.SCALAR)
def make_spec_gross_data(spec):
    frame = {"spec":spec}
    df = pd.DataFrame(frame)
    def make_elements_of_list_into_one_string(origin_list):
        placeholder_word = ' '
        output_sentence = reduce(lambda x,y: x + f"{placeholder_word}" + y ,origin_list)
        return output_sentence
    def extract_gross_data(origin_list):
        sentence = make_elements_of_list_into_one_string(origin_list)
        percent_extract_pattern = r'\d+(\.\d+)?(?=.*?)%'
        remove_placeholder = ''
        if len(re.findall(percent_extract_pattern, sentence)) != 0:
            sentence = re.sub(percent_extract_pattern,remove_placeholder,sentence)
        gross_data_pattern = r'[\+]?(\d+(\.\d+)?)(?!\d+)(\w+)'
        try:
            if len(re.findall(gross_data_pattern, sentence)) == 0:
                gross_data = sentence
            else:
                extract_data_list = re.findall(gross_data_pattern, sentence)
                max_gross_value_units = list(map(lambda x: x[-1], extract_data_list))
                gross_value_list = list(map(lambda x: float(x[0]), extract_data_list))
#                 max_gross_value = max(gross_value_list)
                max_gross_value = gross_value_list[-1]            #单位的优先级还没做，先暂时选最后一位数据作为总量
                max_gross_value_index = gross_value_list.index(max_gross_value)
                max_gross_value_unit = max_gross_value_units[max_gross_value_index]
                gross_data = str(max_gross_value) + max_gross_value_unit
        except:
            gross_data = sentence
        return gross_data
    df['spec_standard_gross'] = df.apply(lambda x: extract_gross_data(x.spec), axis=1)
    return df['spec_standard_gross']

@pandas_udf(StringType(), PandasUDFType.SCALAR)
def make_spec_valid_data(spec,spec_gross_data):
    frame = {"spec":spec,
            "spec_gross_data":spec_gross_data}
    df = pd.DataFrame(frame)
    def make_elements_of_list_into_one_string(origin_list):
        placeholder_word = ' '
        output_sentence = reduce(lambda x,y: x + f"{placeholder_word}" + y ,origin_list)
        return output_sentence
    def remove_gross_from_spec(spec,spec_gross_data):
        try:
            if spec_gross_data in spec:
                spec = [x for x in spec if x != spec_gross_data]
                spec_valid = make_elements_of_list_into_one_string(spec)       
            else:
                spec_valid = ' '
        except:
            spec_valid = ' '
            return spec_valid
        
#     df['valid_data'] = df.apply(lambda x:remove_gross_from_spec(x.spec,x.spec_gross_data), axis=1)
    df['valid_data'] = df.apply(lambda x:x.spec[0], axis=1)
    return df['valid_data']

def make_spec_become_string(df_standard):
    df_standard = df_standard.withColumn("SPEC_STANDARD", make_spec_from_array_into_string(col("SPEC_STANDARD")))
#     df_standard.select("SPEC_STANDARD_ORIGINAL","SPEC_STANDARD","SPEC_STANDARD_GROSS","SPEC_STANDARD_VALID").distinct().show(500)
#     print(df_standard.printSchema())
    return df_standard

@pandas_udf(StringType(), PandasUDFType.SCALAR)
def make_spec_from_array_into_string(spec_standard):
    frame = {"spec_standard":spec_standard}
    df = pd.DataFrame(frame)
    def make_elements_of_list_into_one_string(origin_list):
        placeholder_word = ' '
        try:
            output_sentence = str(reduce(lambda x,y: x + f"{placeholder_word}" + y ,origin_list))
        except:
            output_sentence = ''
        return output_sentence
    df['out_put_col'] = df.apply(lambda x: make_elements_of_list_into_one_string(x.spec_standard), axis=1)
    return df['out_put_col']

###### 写入文件
def write_files(input_dataframe, path_of_write, file_type, repartition_num):
    
    try:
        if file_type.lower() == "parquet":
            input_dataframe.repartition(repartition_num).write.mode("overwrite").parquet(path_of_write)
        else:
            input_dataframe.repartition(1).write.mode("overwrite").csv(path_of_write,header=True)
        message = fr"{path_of_write} {file_type} Write Success!"
    except:
        message = fr"{path_of_write} {file_type} Write Failed!"
    print(message)
    return message
################-----------------------functions---------------------------################

