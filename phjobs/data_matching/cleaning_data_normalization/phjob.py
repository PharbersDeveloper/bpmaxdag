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
    chc_gross_unit_path = kwargs["path_chc_gross_unit"]
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
    df_chc_gross_unit = load_chc_gross_unit(spark, chc_gross_unit_path)
    df_cleanning.persist()
    df_cleanning.write.mode("overwrite").parquet(origin_path)
#########---------------load file------------------------################

#########--------------main function--------------------#################   

    if "code" in df_cleanning.columns:
        #CHC列重命名
        df_cleanning = df_cleanning.withColumnRenamed("code","CODE")
        #DOSAGE预处理
        df_cleanning = make_dosage_standardization(df_cleanning)
        #添加标准总量单位
        df_cleanning = add_chc_standard_gross_unit(df_cleanning,df_chc_gross_unit)
        #SPEC数据预处理
        df_cleanning = pre_to_standardize_data(df_cleanning)
        #基于不同的总量单位进行SPEC数据提取
        df_cleanning = extract_useful_spec_data(df_cleanning)
        #数据提纯
        df_cleanning = make_spec_gross_and_valid_pure(df_cleanning)
        #单位归一化处理
        df_cleanning = make_spec_unit_standardization(df_cleanning)
       #组合成新SPEC
        df_cleanning = create_new_spec_col(df_cleanning)
        #从spec中抽取pack_id
        df_cleanning = get_pack(df_cleanning)
        #干预表逻辑存在问题，需要去掉！！！  
    # 	df_cleanning = human_interfere(df_cleanning, df_interfere) 
        df_cleanning = get_inter(df_cleanning,df_second_interfere)
    else:
        #cpa中spec转化成结构化数据
        df_cleanning = make_cpa_spec_become_structured(df_cleanning)
        #词形还原
        df_cleanning = restore_nonstandard_data_to_normal(df_cleanning)
        #数据单位标准化
        df_cleanning = make_unit_standardization(df_cleanning)
        #spec 有效性和总量拆分
        df_cleanning = extract_spec_valid_and_gross(df_cleanning)
        #spec array转string类型
        df_cleanning = make_spec_become_string(df_cleanning)
        #处理pack_id
        df_cleanning = get_cpa_pack(df_cleanning)
        
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
 
def load_chc_gross_unit(spark,chc_gross_unit_path):
    df_chc_gross_unit = spark.read.parquet(chc_gross_unit_path)
    return df_chc_gross_unit

def make_dosage_standardization(df_cleanning):
    #CHC中DOSAGE干扰项剔除
    replace_dosage_str = r'(([(（].*[)）])|(\s+))'
    df_cleanning = df_cleanning.withColumn("DOSAGE", regexp_replace(col("DOSAGE"),replace_dosage_str,""))
    return df_cleanning

def add_chc_standard_gross_unit(df_cleanning,df_chc_gross_unit):

    df_chc_gross_unit_mg = df_chc_gross_unit.select('CHC_GROSS_UNIT_MG').withColumnRenamed('CHC_GROSS_UNIT_MG','DOSAGE')\
                                            .withColumn('CHC_GROSS_UNIT',lit('MG'))
    df_chc_gross_unit_ml = df_chc_gross_unit.select('CHC_GROSS_UNIT_ML').withColumnRenamed('CHC_GROSS_UNIT_ML','DOSAGE')\
                                            .withColumn('CHC_GROSS_UNIT',lit('ML'))
    df_chc_gross_unit_cm = df_chc_gross_unit.select('CHC_GROSS_UNIT_CM').withColumnRenamed('CHC_GROSS_UNIT_CM','DOSAGE')\
                                            .withColumn('CHC_GROSS_UNIT',lit('CM'))
    df_chc_gross_unit_pen = df_chc_gross_unit.select('CHC_GROSS_UNIT_PEN').withColumnRenamed('CHC_GROSS_UNIT_PEN','DOSAGE')\
                                            .withColumn('CHC_GROSS_UNIT',lit('喷'))

    df_chc_gross_unit = df_chc_gross_unit_mg.union(df_chc_gross_unit_ml).union(df_chc_gross_unit_cm).union(df_chc_gross_unit_pen).filter(col('DOSAGE').isNotNull())
    df_cleanning = df_cleanning.join(df_chc_gross_unit,df_cleanning.DOSAGE == df_chc_gross_unit.DOSAGE , 'left').drop(df_chc_gross_unit.DOSAGE)
    return df_cleanning

def pre_to_standardize_data(df_cleanning):
    #标准表DOSAGE中干扰项剔除
    remove_spec_str = r'(\s+)'
    df_cleanning = df_cleanning.withColumn("SPEC", regexp_replace(col("SPEC"),remove_spec_str,""))\
                                .withColumn("SPEC", upper(col('SPEC')))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(万单位)", "×10MG"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(μ)", "U"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(ΜG)", "MG"))\
                                .withColumn("SPEC", regexp_replace("SPEC" , r"(×)", "x"))\
                                .withColumn("SPEC", regexp_replace("SPEC" , r"((IU)|(AXAI?U))", "U"))\
                                .withColumn("SPEC", regexp_replace("SPEC" , r"(MCI)", "MC"))\
                                .withColumn("SPEC", regexp_replace("SPEC" , r"(M1)" ,"ML"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(揿|掀)", "喷"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(CM2)", "CM"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(∶)", ":"))\
                                .withColumn("SPEC", regexp_replace("SPEC" , r"(克)" ,"G"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(万U)","0000U"))\
                                .withColumn("SPEC", regexp_replace("SPEC", r"(单位)","MG"))
    return df_cleanning

def extract_useful_spec_data(df_cleanning):
    
    #chc总量数据的提取
    extract_spec_value_MG = r'(\d+\.?\d*(((GM)|[MU]?G)|Y|(ΜG)|(万?单位)|(PNA)))'
    extract_spec_value_ML = r'(\d+\.?\d*((M?L)|(PE)))'
    extract_spec_value_U = r'(\d+\.?\d*((I?U)|(TIU)))'
    extract_spec_value_PEN = r'(\d+\.?\d*(喷))'
    extract_spec_value_CM = r'(\d+\.?\d*(CM)?[×:*](\d+\.?\d*(CM)?)([×*](\d+\.?\d*(CM)?))?|(\d+\.?\d*(CM)))'
    extract_pure_spec_valid_value = r'((\d+\.?\d*)((M?L)|([MU]?G)|I?[U喷KY]|(C?M)))'
    df_cleanning = df_cleanning.withColumn('SPEC_GROSS_VALUE', when(col('CHC_GROSS_UNIT') == 'MG' , regexp_extract(col('SPEC'), extract_spec_value_MG, 1))\
                                           .when(col('CHC_GROSS_UNIT') == 'ML' , regexp_extract(col('SPEC'), extract_spec_value_ML, 1))\
                                           .when(col('CHC_GROSS_UNIT') == 'U' , regexp_extract(col('SPEC'), extract_spec_value_U, 1))\
                                           .when(col('CHC_GROSS_UNIT') == '喷' , regexp_extract(col('SPEC'), extract_spec_value_PEN, 1))\
                                           .when(col('CHC_GROSS_UNIT') == 'CM' , regexp_extract(col('SPEC'), extract_spec_value_CM, 1)))\
                                            .withColumn('SPEC_GROSS_VALUE', when( col('SPEC_GROSS_VALUE') == '', regexp_extract(col('SPEC'), extract_pure_spec_valid_value, 1))\
                                            .otherwise(col('SPEC_GROSS_VALUE')))
    print('数据总数：' + str(df_cleanning.count()))
    print('匹配失败数据：' + ' ' + str(df_cleanning.filter(col('SPEC_GROSS_VALUE') == '').count()) ,  '匹配率:' +' ' +  str(( 1 - int(df_cleanning.filter(col('SPEC_GROSS_VALUE') == '').count()) / int(df_cleanning.count())) * 100) + '%' ) 
    df_cleanning.filter(col('SPEC_GROSS_VALUE') == '').groupBy(col("SPEC")).agg(count(col("SPEC"))).show(200)
    #有效性的提取
    extract_spec_valid_value_MG = r'(([:/]\d+.?\d*[UM]?G)|(\d+.?\d*[MΜ]?G[×/](?![M]G))|(每(?!\d+.?\d*[MΜ]?G).*?(\d+.?\d*[MΜ]?G)))'
    extract_spec_valid_value_ML = r'([:]\d+.?\d*(([UM]?G(?![:]))|U)|(\d+.?\d*ΜG/ML)|((\d+.?\d*)ML×\d{1,2})|((\d+.?\d*)ML(?![:(/,含的中)])))'
    extract_spec_valid_value_U = r'(:?(\d+\.?\d*)((MIU)|([UKM](?![LG])U?)))'
    extract_spec_valid_value_PEN = r'(\d+\.?\d*([MΜ]G[//]喷))'
    extract_spec_valid_value_CM = r'(\d+\.?\d*(CM)?[×:*](\d+\.?\d*(CM)?)([×*](\d+\.?\d*(CM)?))?|(\d+\.?\d*(CM)))' 
    df_cleanning = df_cleanning.withColumn("SPEC_VALID_VALUE", when(col('CHC_GROSS_UNIT') == 'MG' , regexp_extract(col('SPEC'), extract_spec_valid_value_MG, 1))\
                                           .when(col('CHC_GROSS_UNIT') == 'ML' , regexp_extract(col('SPEC'), extract_spec_valid_value_ML, 1))\
                                           .when(col('CHC_GROSS_UNIT') == 'U' , regexp_extract(col('SPEC'), extract_spec_valid_value_U, 1))\
                                           .when(col('CHC_GROSS_UNIT') == '喷' , regexp_extract(col('SPEC'), extract_spec_valid_value_PEN, 1))\
                                           .when(col('CHC_GROSS_UNIT') == 'CM' , regexp_extract(col('SPEC'), extract_spec_valid_value_CM, 1)))\
                                            .withColumn('SPEC_VALID_VALUE', when(col('SPEC_VALID_VALUE') == '', col('SPEC_GROSS_VALUE')).otherwise(col('SPEC_VALID_VALUE')))

    return df_cleanning
    
def make_cpa_spec_become_structured(df_cleanning):
    
    df_cleanning = df_cleanning.withColumn('SPEC_ORIGINAL', col("SPEC"))
    df_cleanning = df_cleanning.withColumn("SPEC", remove_spec_spaces_between_values_and_units(col("SPEC")))
    remove_pattern = r'([×*].*)'
    df_cleanning = df_cleanning.withColumn("SPEC", regexp_replace(col("SPEC"),remove_pattern,''))
    split_spec_str = r'(\s+)'
    df_cleanning = df_cleanning.withColumn("SPEC", split(col("SPEC"), split_spec_str,).cast(ArrayType(StringType())))
    stopwords = ['POWD','IN','SOLN','IJ','AERO','CAP','SYRP','OR','EX','PATC','GRAN','OINT','PILL','TAB','SUSP','OP','SL','NA','LSU','']
    remover = StopWordsRemover(stopWords=stopwords, inputCol="SPEC", outputCol="SPEC_TEMP")
    df_cleanning = remover.transform(df_cleanning)
    df_cleanning = df_cleanning.drop("SPEC").withColumnRenamed("SPEC_TEMP","SPEC")
    
    return df_cleanning

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

def make_spec_become_string(df_cleanning):
    df_cleanning = df_cleanning.withColumn("SPEC", make_spec_from_array_into_string(col("SPEC")))
    df_cleanning.select("SPEC_ORIGINAL","SPEC","SPEC_GROSS","SPEC_VALID").distinct().show(500)
    print(df_cleanning.count())
    print(df_cleanning.printSchema())   
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

def make_spec_gross_and_valid_pure(df_cleanning):

    #数据提纯
    extract_pure_spec_valid_value = r'(\d+\.?\d*)((M?L)|([MU]?G)|I?[U喷KY]|(C?M))'
    df_cleanning = df_cleanning.withColumn("SPEC_GROSS_VALUE_PURE", regexp_extract(col("SPEC_GROSS_VALUE"),extract_pure_spec_valid_value,1))\
                                .withColumn("SPEC_GROSS_UNIT_PURE", regexp_extract(col("SPEC_GROSS_VALUE"),extract_pure_spec_valid_value,2))\
                                .withColumn("SPEC_VALID_VALUE_PURE", regexp_extract(col("SPEC_VALID_VALUE"),extract_pure_spec_valid_value,1))\
                                .withColumn("SPEC_VALID_UNIT_PURE", regexp_extract(col("SPEC_VALID_VALUE"),extract_pure_spec_valid_value,2))\
                                .drop(col("SPEC_VALID_VALUE"))
    df_cleanning_ = df_cleanning.select(["SPEC","SPEC_GROSS_VALUE","SPEC_GROSS_VALUE_PURE","SPEC_GROSS_UNIT_PURE","SPEC_VALID_VALUE_PURE","SPEC_VALID_UNIT_PURE"]).distinct()   
    return df_cleanning

def make_spec_unit_standardization(df_cleanning):
    df_cleanning = df_cleanning.withColumn("SPEC_GROSS_VALUE_PURE", col("SPEC_GROSS_VALUE_PURE").cast("double"))\
                                .withColumn("SPEC_VALID_VALUE_PURE", col("SPEC_VALID_VALUE_PURE").cast("double"))

#总量数值归一化
    df_cleanning = df_cleanning.withColumn("SPEC_GROSS_VALUE_PURE",when(col("SPEC_GROSS_UNIT_PURE") == "G", col("SPEC_GROSS_VALUE_PURE")*int(1000))\
                                           .when(col("SPEC_GROSS_UNIT_PURE") == "UG", col("SPEC_GROSS_VALUE_PURE")*int(0.001))\
                                           .when(col("SPEC_GROSS_UNIT_PURE") == "Y", col("SPEC_GROSS_VALUE_PURE")*int(0.001))\
                                           .when(col("SPEC_GROSS_UNIT_PURE") == "L", col("SPEC_GROSS_VALUE_PURE")*int(1000))\
                                           .when(col("SPEC_GROSS_UNIT_PURE") == "MU", col("SPEC_GROSS_VALUE_PURE")*int(1000000))\
                                           .when(col("SPEC_GROSS_UNIT_PURE") == "MIU", col("SPEC_GROSS_VALUE_PURE")*int(1000000))\
                                           .when(col("SPEC_GROSS_UNIT_PURE") == "K", col("SPEC_GROSS_VALUE_PURE")*int(1000))\
                                           .otherwise(col("SPEC_GROSS_VALUE_PURE")))
#有效性数值归一化
    df_cleanning = df_cleanning.withColumn("SPEC_VALID_VALUE_PURE",when(col("SPEC_VALID_UNIT_PURE") == "G", col("SPEC_VALID_VALUE_PURE")*int(1000))\
                                           .when(col("SPEC_VALID_UNIT_PURE") == "UG", col("SPEC_VALID_VALUE_PURE")*int(0.001))\
                                           .when(col("SPEC_VALID_UNIT_PURE") == "Y", col("SPEC_VALID_VALUE_PURE")*int(0.001))\
                                           .when(col("SPEC_VALID_UNIT_PURE") == "L", col("SPEC_VALID_VALUE_PURE")*int(1000))\
                                           .when(col("SPEC_VALID_UNIT_PURE") == "MU", col("SPEC_VALID_VALUE_PURE")*int(1000000))\
                                           .when(col("SPEC_VALID_UNIT_PURE") == "MIU", col("SPEC_VALID_VALUE_PURE")*int(1000000))\
                                           .when(col("SPEC_VALID_UNIT_PURE") == "K", col("SPEC_VALID_VALUE_PURE")*int(1000))\
                                           .otherwise(col("SPEC_VALID_VALUE_PURE")))
# 有效性单位归一化
    replace_spec_value_MG = r'(((GM)|[MU]?G)|Y)'
    df_cleanning = df_cleanning.withColumn("SPEC_VALID_UNIT_PURE", regexp_replace(col("SPEC_VALID_UNIT_PURE"),replace_spec_value_MG,'MG'))
# 	df_cleanning.select(["SPEC","SPEC_GROSS_VALUE","SPEC_GROSS_VALUE_PURE","SPEC_GROSS_UNIT_PURE","SPEC_VALID_VALUE_PURE","SPEC_VALID_UNIT_PURE"]).distinct().show(100)
#删除辅助列
    df_cleanning = df_cleanning.withColumnRenamed("SPEC","SPEC_ORIGINAL").drop("SPEC_GROSS_VALUE","SPEC_GROSS_UNIT_PURE")
    return df_cleanning

def create_new_spec_col(df_cleanning):
    df_cleanning = df_cleanning.withColumn("SPEC",concat_ws("/",col("SPEC_VALID_VALUE_PURE"),col("SPEC_VALID_UNIT_PURE"),col("SPEC_GROSS_VALUE_PURE"),col("CHC_GROSS_UNIT")))
    col_list = ['MOLE_NAME', 'PRODUCT_NAME', 'DOSAGE', 'SPEC', 'MANUFACTURER_NAME', 'PACK_QTY', 'PACK_ID_CHECK', 'CODE', 'ID',  'SPEC_GROSS_VALUE_PURE', 'CHC_GROSS_UNIT','SPEC_VALID_VALUE_PURE', 'SPEC_VALID_UNIT_PURE', 'SPEC_ORIGINAL']
    df_cleanning = df_cleanning.select(col_list) 
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

#抽取spec中pack_id数据
def get_pack(df_cleanning):
    extract_pack_id = r'[×x](\d+)./.'
    df_cleanning = df_cleanning.withColumnRenamed("PACK_QTY", "PACK_QTY_ORIGINAL")
    df_cleanning = df_cleanning.withColumn("PACK_QTY", regexp_extract(col("SPEC_ORIGINAL"), extract_pack_id, 1).cast('float'))
    df_cleanning = df_cleanning.withColumn("PACK_QTY", when(col("PACK_QTY").isNull(), col("PACK_QTY_ORIGINAL")).otherwise(col("PACK_QTY"))).drop(col("PACK_QTY_ORIGINAL"))
    return df_cleanning
################----------------------functions------------------------------################