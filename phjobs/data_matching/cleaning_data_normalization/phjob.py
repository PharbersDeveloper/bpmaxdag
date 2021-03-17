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
from pyspark.sql.functions import col , concat , concat_ws
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.functions import udf, pandas_udf, PandasUDFType
from pyspark.sql.functions import split ,count
from pyspark.sql.functions import regexp_replace, upper, regexp_extract
from pyspark.sql.functions import when , lit
from pyspark.ml.feature import StopWordsRemover

def execute(**kwargs):
    """
        please input your code below
        get spark session: spark = kwargs["spark"]()
    """
    logger = phs3logger(kwargs["job_id"])
    spark = kwargs["spark"]()
    logger.info(kwargs)

############-----------input-------------------------###################
    raw_data_path = kwargs["path_cleaning_data"]
    interfere_path = kwargs["path_human_interfere"]
    second_interfere_path = kwargs["path_second_human_interfere"]
    source_data_type = kwargs["source_data_type"]
############-----------input-------------------------###################

###########------------output------------------------###################
    job_id = get_job_id(kwargs)
    run_id = get_run_id(kwargs)
    result_path_prefix = get_result_path(kwargs, run_id, job_id)
    result_path = result_path_prefix + kwargs["cleaning_result"]
    origin_path = result_path_prefix + kwargs["cleaning_origin"]
#########--------------output------------------------####################

###########--------------load file----------------------- ################
    df_cleanning = modify_pool_cleanning_prod(spark, raw_data_path)
    df_interfere = load_interfere_mapping(spark, interfere_path)
    df_second_interfere = load_second_interfere(spark,second_interfere_path)
    df_cleanning.persist()
    df_cleanning.write.mode("overwrite").parquet(origin_path)
#########---------------load file------------------------################

#########--------------main function--------------------#################   
    #spec预处理
    df_cleanning = make_spec_pre_treatment(df_cleanning)

    #源数据判断
    df_cleanning = judge_source_type(df_cleanning, source_data_type)

    #cpa中spec转化成结构化数据
    df_cleanning = make_cpa_spec_become_structured(df_cleanning)

    #spec停用词处理
    df_cleanning = from_spec_remove_stopwords(df_cleanning)

    #SPEC处理
    df_cleanning = make_spec_become_normal(df_cleanning)

    #处理pack_id
    df_cleanning = choose_correct_pack_id(df_cleanning,source_data_type)

    df_cleanning = get_inter(df_cleanning,df_second_interfere)
    df_cleanning = select_cpa_col(df_cleanning)
    df_cleanning.write.mode("overwrite").parquet(result_path)
    
########------------main fuction-------------------------################
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
更高的并发数
"""
def modify_pool_cleanning_prod(spark, raw_data_path):
#     raw_data_path = "s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/zyyin/azsanofi/raw_data"
#     raw_data_path = "s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/zyyin/qilu/raw_data2"
#     raw_data_path = "s3a://ph-max-auto/2020-08-11/BPBatchDAG/refactor/zyyin/eia/raw_data_2"
#     raw_data_path = 's3a://ph-max-auto/2020-08-11/data_matching/refactor/data/CHC/*'
    raw_data_path = r's3a://ph-max-auto/2020-08-11/data_matching/temp/mzhang/test_run_data/chc/0.01'
    if raw_data_path.endswith(".csv"):
        df_cleanning = spark.read.csv(path=raw_data_path, header=True).withColumn("ID", pudf_id_generator(col("MOLE_NAME")))
    else:
        df_cleanning = spark.read.parquet(raw_data_path).withColumn("ID", pudf_id_generator(col("MOLE_NAME")))
    return df_cleanning

@pandas_udf(StringType(), PandasUDFType.SCALAR)
def pudf_id_generator(oid):
    frame = {
        "_ID": oid
            }
    df = pd.DataFrame(frame)
    df["RESULT"] = df["_ID"].apply(lambda x: str(uuid.uuid4()))
    return df["RESULT"]

def load_second_interfere(spark,second_interfere_path):
    df_second_interfere = spark.read.parquet(second_interfere_path)
    return df_second_interfere
 

#spec预处理
def make_spec_pre_treatment(df_cleanning):
    
    df_cleanning = df_cleanning.withColumn('SPEC_ORIGINAL', col("SPEC"))
   
    return df_cleanning

#数据源判断
def judge_source_type(df_cleanning,source_data_type):
    
    if source_data_type.upper() == "CHC": 
        df_cleanning = make_chc_spec_become_normal(df_cleanning)
   
    elif source_data_type.upper() == "AZ":
        df_cleanning = make_az_spec_become_normal(df_cleanning)
    elif source_data_type.upper() == "QILU":
        df_cleanning = make_qilu_spec_become_normal(df_cleanning)
    elif source_data_type.upper() == "EISAI":
        df_cleanning = make_eisai_spec_become_normal(df_cleanning)
    else:
        print("source_data_error!")
    
    return df_cleanning

def make_cpa_spec_become_structured(df):

    split_spec_str = r'(\s+)'
    df = df.withColumn("SPEC", split(col("SPEC"), split_spec_str,).cast(ArrayType(StringType())))

    return df

#处理chc
def make_chc_spec_become_normal(df_cleanning):
         
    remove_pattern = r'([×*].*)'

    df_cleanning = df_cleanning.withColumn("SPEC", regexp_replace(col("SPEC"),remove_pattern,""))
    df_cleanning = df_cleanning.withColumn("SPEC", regexp_replace("SPEC", r"(万单位)", "0000U"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(μ)", "u"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(μg|毫克)", "mg"))\
                                .withColumn("SPEC", regexp_replace("SPEC" , r"(×)", "x"))\
                                .withColumn("SPEC", regexp_replace("SPEC" , r"((iu)|(axai?u))", "u"))\
                                .withColumn("SPEC", regexp_replace("SPEC" , r"(mci)", "mc"))\
                                .withColumn("SPEC", regexp_replace("SPEC" , r"(m1)" ,"ml"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(揿|掀)", "喷"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(cm2)", "cm"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(∶)", ":"))\
                                .withColumn("SPEC", regexp_replace("SPEC" , r"(克)" ,"g"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(万u)","0000u"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(单位)","U"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(ΜG)","MG"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"((?=\d+)万)","0000"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(复方)","CO"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(微)","U"))
    df_cleanning = df_cleanning.withColumn("SPEC", upper(df_cleanning.SPEC))
    df_cleanning = df_cleanning.withColumn("SPEC",extract_spec_values_and_units_from_chcType(col("SPEC")) )
   
    return df_cleanning

#az spec处理
def make_az_spec_become_normal(df_cleanning):
    
    df_cleanning = df_cleanning.withColumn("SPEC", upper(df_cleanning.SPEC))
    df_cleanning = df_cleanning.withColumn("SPEC", remove_spec_spaces_between_values_and_units(col("SPEC")))
    
    return df_cleanning

#齐鲁spec处理
def make_qilu_spec_become_normal(df_cleanning):
    
    df_cleanning = df_cleanning.withColumn("SPEC", upper(df_cleanning.SPEC))
    df_cleanning = df_cleanning.withColumn("SPEC", regexp_replace("SPEC", r"(万单位)", "0000U"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(单位)","U"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(ΜG)","MG"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"((?=\d+)万)","0000"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(复方)","CO"))\
    
    df_cleanning = df_cleanning.withColumn("SPEC", extract_spec_values_and_units_from_qiluType(col("SPEC")))
   
    return df_cleanning

#卫材spec处理
def make_eisai_spec_become_normal(df_cleanning):
     
    df_cleanning = df_cleanning.withColumn("SPEC", upper(df_cleanning.SPEC))
    df_cleanning = df_cleanning.withColumn("SPEC", regexp_replace("SPEC", r"(万单位)", "0000U"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(单位)","U"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(ΜG)","MG"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"((?=\d+)万)","0000"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(复方)","CO"))\

    df_cleanning = df_cleanning.withColumn("SPEC", extract_spec_values_and_units_from_qiluType(col("SPEC")))
   
    return df_cleanning

#spec停用词处理
def from_spec_remove_stopwords(df):
    
    stopwords = ['POWD','IN','SOLN','IJ','AERO','CAP','SYRP','OR','EX','PATC','GRAN','OINT','PILL','TAB','SUSP','OP','SL','NA','LSU','']
    remover = StopWordsRemover(stopWords=stopwords, inputCol="SPEC", outputCol="SPEC_TEMP")
    df = remover.transform(df)
    df = df.drop("SPEC").withColumnRenamed("SPEC_TEMP","SPEC")
    
    return df
   
#SPEC处理
def make_spec_become_normal(df_cleanning):
    
    #词形还原
    df_cleanning = restore_nonstandard_data_to_normal(df_cleanning)
    #数据单位标准化
    df_cleanning = make_unit_standardization(df_cleanning)
           
    #spec 有效性和总量拆分
    df_cleanning = extract_spec_valid_and_gross(df_cleanning)
    #spec array转string类型
    df_cleanning = make_spec_become_string(df_cleanning)

    return df_cleanning

#处理CHC类型数据 
@pandas_udf(StringType(),PandasUDFType.SCALAR)
def extract_spec_values_and_units_from_chcType(origin_col):
    frame = {"origin_col":origin_col}
    df = pd.DataFrame(frame)
    
    def make_elements_of_list_into_one_string(origin_list):
        placeholder_word = ' '
        output_sentence = reduce(lambda x, y: x + f"{placeholder_word}" + y, origin_list)
        return output_sentence

    def add_chc_gross_and_valid(input_gross_pattern,input_valid_pattern,input_string):
        try:
            gross_string = re.findall(input_gross_pattern, input_string)
            gross_data = list(map(lambda x: x[0] + x[-1], gross_string))[0]
            valid_data = re.findall(input_valid_pattern, input_string)
            valid_list = list(map(lambda x: x[0] + x[-1], valid_data))
            valid_list.append(gross_data)
            try:
                file = make_elements_of_list_into_one_string(valid_list)
            except:
                file = input_string
        except:
            file = input_string
        return file

    def extract_chc_values_and_unit(input_string):

        pattern_gross = r'(\d+(\.\d+)?)([GUMYLIKAX]+):\d+'
        pattern_gross_type2 = r'(\d+(\.\d+)?)([IGUMYLKAX]+)[\(].*?:.*?\d+[GUMYLKAX]+(?=[\)])'
        pattern_gross_type3 = r"(\d+(\.\d+)?)([IGUMYLKAX]+)[\(].*?\/(?=\d+).*[GUMYLKAX]+(?=[\)])"
        pattern_gross_type4 = r'(\d+(\.\d+)?)([IGUMYLKAX]+)\(相当于.*\)'
        pattern_gross_type5 = r'(\d+(\.\d+)?)([IGUMYLKAX]+)'
        #类型1  0.643G(0.6G:0.043G)
        if len(re.findall(pattern_gross_type2,input_string)) != 0:
            valid_pattern = r'\(.*?:(\d+(\.\d+)?)([IGUMYLKAX]+)(?=\))'
            output_file = add_chc_gross_and_valid(input_gross_pattern=pattern_gross_type2,input_valid_pattern=valid_pattern,input_string=input_string)

        #类型2 15G:15MG
        elif len(re.findall(pattern_gross,input_string)) != 0:
            valid_pattern = r':(\d+(\.\d+)?)([GUMYLIKAX]+)'

            output_file = add_chc_gross_and_valid(input_gross_pattern=pattern_gross, input_valid_pattern=valid_pattern,
                                              input_string=input_string)
        #类型  3156.25MG(125MG/31.25MG)
        elif len(re.findall(pattern_gross_type3,input_string)) != 0:

            valid_pattern = r'[\(].*?\/(\d+(\.\d+)?)([IGUMYLKAX]+)(?=[\)])'
            output_file = add_chc_gross_and_valid(input_gross_pattern=pattern_gross_type3, input_valid_pattern=valid_pattern,
                                                  input_string=input_string)

        #类型4 10G(相当于原生药14G)×20袋/盒
        elif len(re.findall(pattern_gross_type4,input_string)) != 0:
            valid_pattern = r'\(相当于.*?(\d+(\.\d+)?)([GIUMYLKAX]+)\)'
            output_file = add_chc_gross_and_valid(input_gross_pattern=pattern_gross_type4,
                                                  input_valid_pattern=valid_pattern,
                                                  input_string=input_string)
            #取所有的数值单位类型
        elif len(re.findall(pattern_gross_type5,input_string)) != 0:
            string_list = re.findall(pattern_gross_type5,input_string)
            gross_string = list(map(lambda x:x[0]+x[-1],string_list))
            output_file = make_elements_of_list_into_one_string(gross_string)
        #否则返回原值
        else:
            output_file = input_string
        return output_file
    df['output_col'] = df.apply(lambda x:extract_chc_values_and_unit(x.origin_col) ,axis=1)
    
    return df['output_col']


@pandas_udf(StringType(),PandasUDFType.SCALAR)
def remove_spec_spaces_between_values_and_units(origin_col):
    frame = {"origin_col":origin_col}
    df = pd.DataFrame(frame)
    
    def make_elements_of_list_into_one_string(input_list):
        placeholder_word = ' '
        output_sentence = reduce(lambda x,y: x + f"{placeholder_word}" + y ,input_list)
        return output_sentence
    
    def remove_spaces_between_values_and_units(input_sentence):
        remove_space = r'(\d+(\.\d+)?)\s+(\w+)'
        data_list = re.findall(remove_space,input_sentence)
        if len(data_list) == 0:
            output_sentence = input_sentence
        else:
            output_list = list(map(lambda x: x[0]+ x[-1], data_list))
            output_sentence = make_elements_of_list_into_one_string(output_list)   
        return output_sentence
    df['output_col'] = df.apply(lambda x: remove_spaces_between_values_and_units(x.origin_col), axis =1)
    
    return df['output_col'] 

#处理齐鲁类型数据
@pandas_udf(StringType(),PandasUDFType.SCALAR)
def extract_spec_values_and_units_from_qiluType(origin_col):
    frame = {"origin_col":origin_col}
    df = pd.DataFrame(frame)
    
    def make_elements_of_list_into_one_string(origin_list):
        placeholder_word = ' '
        output_sentence = reduce(lambda x, y: x + f"{placeholder_word}" + y, origin_list)
        return output_sentence
    
    def extract_all_qilu_spec_data(pattern_gross_type,input_string):
        try:
            string_list = re.findall(pattern_gross_type,input_string)
            gross_string = list(map(lambda x: x[0] + x[-1], string_list))
            output_file = make_elements_of_list_into_one_string(gross_string)
        except:
            output_file = input_string
        return output_file

    def add_qilu_gross_and_valid(gross_pattern,valid_split_pattern,valid_pattern,input_string):
        try:
            gross_string = re.findall(gross_pattern, input_string)
            gross_data = list(map(lambda x: x[0] + x[-1], gross_string))[0]
            valid_string = re.split(valid_split_pattern,input_string)[-1]
            valid_data_list = re.findall(valid_pattern, valid_string)
            output_data_list = list(map(lambda x: x[0] + x[-1], valid_data_list))
            output_data_list.append(gross_data)
            try:
                file = make_elements_of_list_into_one_string(output_data_list)
            except:
                file = input_string
        except:
            file = input_string
        return file

    def extract_qilu_values_and_unit(input_string):

        pattern_gross = r'(\d+(\.\d+)?)(\w+).*[(].*[)]'
        pattern_gross_type2 = r'(\d+(\.\d+)?)(\w+)[∶]'
        pattern_gross_type3 = r"(\d+(\.\d+)?)[万]U"
        pattern_gross_type4 = r'(\d+(\.\d+)?)([GUMYLKAX]+)'
        
        #类型一 50ml∶单硝酸异山梨酯20mg,葡萄糖12.5g 、 1ml：0.1mg
        if len(re.findall(pattern_gross_type2,input_string)) != 0:
            valid_split_pattern = r'∶'
            valid_pattern = r'(\d+(\.\d+)?)([GUMYLKAX]+)'
            output_file = add_qilu_gross_and_valid(gross_pattern=pattern_gross_type2, valid_split_pattern=valid_split_pattern,\
                                                   valid_pattern=valid_pattern, input_string=input_string)
        #类型二 1.5g(头孢哌酮0.75g，舒巴坦0.75g)
        elif len(re.findall(pattern_gross,input_string)) != 0:
            valid_split_pattern = r'\('
            valid_pattern = r'(\d+(\.\d+)?)([GUMYLKAX]+)'
            output_file = add_qilu_gross_and_valid(gross_pattern=pattern_gross, valid_split_pattern=valid_split_pattern,\
                                                   valid_pattern=valid_pattern, input_string=input_string)

        #类型三 50万U
        elif len(re.findall(pattern_gross_type3,input_string)) != 0:
            try:
                output_string = re.sub('万','0000',input_string)
                output_file = extract_all_qilu_spec_data(pattern_gross_type=pattern_gross_type4, input_string=output_string)
            except:
                output_file = input_string

        #取所有的数值单位类型
        elif len(re.findall(pattern_gross_type4,input_string)) != 0:
            output_file =extract_all_qilu_spec_data(pattern_gross_type=pattern_gross_type4, input_string=input_string)

         #否则返回原值
        else:
            output_file = input_string
        return output_file

    df['output_col'] = df.apply(lambda x: extract_qilu_values_and_unit(x.origin_col), axis=1)
    return df['output_col']

@pandas_udf(ArrayType(StringType()),PandasUDFType.SCALAR)
def remove_spec_space_element(origin_spec):
    frame = {"origin_spec":origin_spec}
    df = pd.DataFrame(frame)
    def remove_space_element(input_list):
        out_put_list = [x for x in input_list if len(x) != 0]
        return out_put_list
    df['out_put_spec'] = df.apply(lambda x: np.array(remove_space_element(x.origin_spec)), axis=1)
    
    return df['out_put_spec']

#处理spec中非标准数据
def restore_nonstandard_data_to_normal(df_cleanning):
    df_cleanning = df_cleanning.withColumn("SPEC", make_nonstandard_data_become_normal_addType(col("SPEC")))
    df_cleanning = df_cleanning.withColumn("SPEC", make_nonstandard_data_become_normal_percent_or_rateType(col("SPEC")))
    
    return df_cleanning

#处理spec中add类型数据
@pandas_udf(ArrayType(StringType()), PandasUDFType.SCALAR)
def make_nonstandard_data_become_normal_addType(origin_col):
    def judge_nonstandard_data(word):
        data_extraction_rule = r'(\d+(\.\d+)?)([GIUMYLKAX]+(?=\+))'
        if len(re.findall(data_extraction_rule, word)) != 0:
            the_first_data = re.findall(data_extraction_rule, word)[0]
            the_first_data_value = the_first_data[0]
            the_first_data_unit = the_first_data[-1]
            the_test_of_data_extract_rule = r'\+(\d+(\.\d+)?)([GIUMYLKAX]+)' 
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
        gross_data_pattern = r'[\+]?(\d+(\.\d+)?)(?!\d+)([GIUMYLKAX]+)'
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
        gross_data_pattern = r'[\+]?(\d+(\.\d+)?)([GIUMYLKAX]+)'
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
def make_unit_standardization(df_cleanning):
    
    df_cleanning = df_cleanning.withColumn("SPEC", create_values_and_units(col("SPEC")))
    
    return df_cleanning


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
        
        pattern_decimal = r'(\d+\.\d+)([GIUMYLKAX]+)$'
        try:
            if re.match(pattern_decimal, word) == None:
                pattern_decimal = r'(\d+)([GIUMYLKAX]+)$'
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


def extract_spec_valid_and_gross(df_cleanning):    
    
    df_cleanning = df_cleanning.withColumn("SPEC_GROSS", make_spec_gross_data(col("SPEC")))
    df_cleanning = df_cleanning.withColumn("SPEC_VALID", make_spec_valid_data(col("SPEC"),col("SPEC_GROSS")))
    
    return df_cleanning


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
        gross_data_pattern = r'[\+]?(\d+(\.\d+)?)(?!\d+)([GIUMYLKAX]+)'
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

def make_spec_become_string(df_cleanning):
    df_cleanning = df_cleanning.withColumn("SPEC", make_spec_from_array_into_string(col("SPEC")))
    return df_cleanning

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

#pakc_id 处理
def choose_correct_pack_id(df_cleanning,source_data_type):
    
#     df_cleanning.select("SPEC_ORIGINAL").distinct().show(500)
    if source_data_type.upper() == 'CHC':
        df_cleanning = df_cleanning.withColumn("PACK_QTY",extract_chc_pack_id_from_spec(df_cleanning.SPEC_ORIGINAL,df_cleanning.PACK_QTY))
    else:
        df_cleanning = get_cpa_pack(df_cleanning)
    
    return df_cleanning

@pandas_udf(StringType(),PandasUDFType.SCALAR)
def extract_chc_pack_id_from_spec(spec_original, pack_qty):
    frame = {"spec_original":spec_original,
            "pack_qty":pack_qty}
    df = pd.DataFrame(frame)
    def extract_regex_pack_id(word,pack_original):
        pack_id_pattern = r'×(\d+)'
        try:
            if (re.findall(pack_id_pattern, word)) != 0:
                pack_id = str(float(re.findall(pack_id_pattern,word)[0]))
            else:
                pack_id = str(float(pack_original))
        except:
            try:
                pack_id = str(float(pack_original))
            except:
                pack_id = str(float(1.0))
        
        return pack_id
        
    df['pack_id'] = df.apply(lambda x: str(extract_regex_pack_id(x.spec_original, x.pack_qty)), axis=1)
    
    return df['pack_id']

def get_cpa_pack(df_cleanning):
    extract_pack_id = r'[×*](\d+)'
    df_cleanning = df_cleanning.withColumnRenamed("PACK_QTY", "PACK_QTY_ORIGINAL")
    df_cleanning = df_cleanning.withColumn("PACK_QTY", regexp_extract(col("SPEC_ORIGINAL"), extract_pack_id, 1).cast('float'))
    df_cleanning = df_cleanning.withColumn("PACK_QTY", when(col("PACK_QTY").isNull(), col("PACK_QTY_ORIGINAL")).otherwise(col("PACK_QTY"))).drop(col("PACK_QTY_ORIGINAL"))
    return df_cleanning

def get_pca_inter(df_cleanning,df_second_interfere):
    df_cleanning = df_cleanning.join(df_second_interfere, df_cleanning.MOLE_NAME == df_second_interfere.MOLE_NAME_LOST, 'left')
    df_cleanning = df_cleanning.withColumn('new', when(df_cleanning.MOLE_NAME_LOST.isNull(), df_cleanning.MOLE_NAME)\
                                           .otherwise(df_cleanning.MOLE_NAME_STANDARD))\
                                            .drop("MOLE_NAME", "MOLE_NAME_LOST", "MOLE_NAME_STANDARD")\
                                            .withColumnRenamed("new", "MOLE_NAME")
    return df_cleanning 

def select_cpa_col(df_cleanning):
    cpa_cols =['MOLE_NAME','PRODUCT_NAME', 'DOSAGE', 'SPEC', 'PACK_QTY', 'MANUFACTURER_NAME', 'PACK_ID_CHECK', 'ID','SPEC_ORIGINAL','SPEC_VALID','SPEC_GROSS']
    df_cleanning = df_cleanning.select(cpa_cols)
    return df_cleanning

"""
读取人工干预表
"""
def load_interfere_mapping(spark, human_replace_packid_path):

    df_interfere = spark.read.parquet(human_replace_packid_path) \
                            .withColumnRenamed("match_MOLE_NAME_CH", "MOLE_NAME_INTERFERE") \
                            .withColumnRenamed("match_PRODUCT_NAME", "PRODUCT_NAME_INTERFERE")  \
                            .withColumnRenamed("match_SPEC", "SPEC_INTERFERE") \
                            .withColumnRenamed("match_DOSAGE", "DOSAGE_INTERFERE") \
                            .withColumnRenamed("match_PACK_QTY", "PACK_QTY_INTERFERE") \
                            .withColumnRenamed("match_MANUFACTURER_NAME_CH", "MANUFACTURER_NAME_INTERFERE") \
                            .withColumnRenamed("PACK_ID", "PACK_ID_INTERFERE")
    return df_interfere

def human_interfere(df_cleanning, df_interfere):
    # 1. 人工干预优先，不太对后期改
    # 干预流程将数据直接替换，在走平常流程，不直接过滤，保证流程的统一性
    df_cleanning = df_cleanning.withColumn("min", concat(df_cleanning["MOLE_NAME"], df_cleanning["PRODUCT_NAME"], df_cleanning["SPEC"], \
                                                         df_cleanning["DOSAGE"], df_cleanning["PACK_QTY"], df_cleanning["MANUFACTURER_NAME"]))

    # 2. join 干预表，替换原有的原始数据列
    df_cleanning = df_cleanning.join(df_interfere, on="min",  how="leftouter") \
                                .na.fill({
                                "MOLE_NAME_INTERFERE": "unknown",
                                "PRODUCT_NAME_INTERFERE": "unknown",
                                "SPEC_INTERFERE": "unknown",
                                "DOSAGE_INTERFERE": "unknown",
                                "PACK_QTY_INTERFERE": "unknown",
                                "MANUFACTURER_NAME_INTERFERE": "unknown"})

    df_cleanning = df_cleanning.withColumn("MOLE_NAME", interfere_replace_udf(df_cleanning.MOLE_NAME, df_cleanning.MOLE_NAME_INTERFERE)) \
                                .withColumn("PRODUCT_NAME", interfere_replace_udf(df_cleanning.PRODUCT_NAME, df_cleanning.PRODUCT_NAME_INTERFERE)) \
                                .withColumn("SPEC", interfere_replace_udf(df_cleanning.SPEC, df_cleanning.SPEC_INTERFERE)) \
                                .withColumn("DOSAGE", interfere_replace_udf(df_cleanning.DOSAGE, df_cleanning.DOSAGE_INTERFERE)) \
                                .withColumn("PACK_QTY", interfere_replace_udf(df_cleanning.PACK_QTY, df_cleanning.PACK_QTY_INTERFERE)) \
                                .withColumn("MANUFACTURER_NAME", interfere_replace_udf(df_cleanning.MANUFACTURER_NAME, df_cleanning.MANUFACTURER_NAME_INTERFERE))
    df_cleanning = df_cleanning.select("ID", "PACK_ID_CHECK", "MOLE_NAME", "PRODUCT_NAME", "DOSAGE", "SPEC", "PACK_QTY", "MANUFACTURER_NAME")
    return df_cleanning
 
@udf(returnType=StringType())
def interfere_replace_udf(origin, interfere):
    if interfere != "unknown":
        origin = interfere
    return origin

def get_inter(df_cleanning,df_second_interfere):
    df_cleanning = df_cleanning.join(df_second_interfere, df_cleanning.MOLE_NAME == df_second_interfere.MOLE_NAME_LOST, 'left')
    df_cleanning = df_cleanning.withColumn('new', when(df_cleanning.MOLE_NAME_LOST.isNull(), df_cleanning.MOLE_NAME)\
                                           .otherwise(df_cleanning.MOLE_NAME_STANDARD))\
                                            .drop("MOLE_NAME", "MOLE_NAME_LOST", "MOLE_NAME_STANDARD")\
                                            .withColumnRenamed("new", "MOLE_NAME")
    return df_cleanning

def make_dosage_standardization(df_cleanning):
    #CHC中DOSAGE干扰项剔除
    replace_dosage_str = r'(([(（].*[)）])|(\s+))'
    df_cleanning = df_cleanning.withColumn("DOSAGE", regexp_replace(col("DOSAGE"),replace_dosage_str,""))
    return df_cleanning

################----------------------functions------------------------------################