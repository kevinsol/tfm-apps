#!/usr/bin/env python
# coding: utf-8
# load_dims.py
# Author: Kevin Solano


from modulos.create_sparkSession import create_spark_session
from pyspark.sql.functions import *
from pyspark.sql import types as t
import configparser

def load_dim_job():
    """    
    proceso de carga de dim_job 
    """
    config = configparser.ConfigParser()
    config.read('/home/jovyan/work/tfm-jobs-main/parameters.cfg')
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()
    # Lectura de datos de staging layer
    df_stg_jobposts = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/job_posts")
        
    if loadType=="full":
        #DIM JOB POST
        # transformando la dimensión
        df_dim = df_stg_jobposts.select("job_title", "job_category1").distinct() \
        .withColumn("job_key", expr("uuid()")) \
        .withColumnRenamed("job_category1", "job_category") \
        .withColumn("created_date", current_date()) \
        .select("job_key", "job_title", "job_category", "created_date") 
        
        df_dim.show(5)
        
        # carga dimensión
        df_dim.repartition(2).write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/dim_job", mode="overwrite")
                
    else:        
        # Lectura de datos        
        df_act_dim = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/dim_job")

        # transformando la dimensión
        df_dim = df_stg_jobposts.select("job_title", "job_category1").distinct() \
        .withColumnRenamed("job_category1", "job_category") \
        .withColumn("created_date", current_date()) \
        .select("job_title", "job_category", "created_date")              
        
        # indentificando incremental de datos
        df_delta_dim = df_dim.join(df_act_dim, [df_dim["job_title"] == df_act_dim["job_title"],
                                                df_dim["job_category"] == df_act_dim["job_category"]],"leftanti") \
        .withColumn("job_key", expr("uuid()")) \
        .select("job_key", "job_title", "job_category", "created_date") 

        df_new_dim = df_act_dim.union(df_delta_dim)

        df_new_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/dim_job", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/dim_job") \
        .repartition(2).write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/dim_job", mode="overwrite")

def load_dim_city():
    """    
    carga de dimensión de ciudad
    """
    config = configparser.ConfigParser()    
    config.read('/home/jovyan/work/tfm-jobs-main/parameters.cfg')    
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()    
    # Lectura de datos de staging layer
    df_stg_jobposts = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/job_posts")
        
    if loadType=="full":
        #DIM CITY        
        # transformando la dimensión
        df_dim = df_stg_jobposts.select("city").distinct() \
        .withColumn("city_key", expr("uuid()")) \
        .withColumn("country", lit("EGIPTO")) \
        .withColumn("created_date", current_date()) \
        .select("city_key", "city", "country","created_date")
        
        df_dim.show(5)
        
        # carga dimensión
        df_dim.repartition(2).write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/dim_city", mode="overwrite")
                
    else:        
        # Lectura de datos        
        df_act_dim = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/dim_city")

        # transformando la dimensión
        df_dim = df_stg_jobposts.select("city").distinct() \
            .withColumn("country", lit("EGIPTO")) \
            .withColumn("created_date", current_date()) \
            .select("city", "country", "created_date")                
        
        # indentificando incremental de datos
        df_delta_dim = df_dim.join(df_act_dim, df_dim["city"] == df_act_dim["city"],"leftanti") \
            .withColumn("city_key", expr("uuid()")) \
            .select("city_key", "city", "country", "created_date")

        df_new_dim = df_act_dim.union(df_delta_dim)

        df_new_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/dim_city", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/dim_city") \
            .repartition(2).write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/dim_city", mode="overwrite")

def load_dim_industry():
    """    
    proceso de carga de dim_industry
    """
    config = configparser.ConfigParser()    
    config.read('/home/jovyan/work/tfm-jobs-main/parameters.cfg')    
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()    
    # Lectura de datos de staging layer
    df_stg_jobposts = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/job_posts")
    
    if loadType=="full":        
        # transformando la dimensión
        df_dim = df_stg_jobposts.select("job_industry1").distinct() \
            .withColumn("industry_key", expr("uuid()")) \
            .withColumnRenamed("job_industry1", "job_industry") \
            .withColumn("created_date", current_date()) \
            .select("industry_key", "job_industry", "created_date")
        df_dim.show(5)
        
        # carga dimensión
        df_dim.repartition(2).write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/dim_industry", mode="overwrite")
        
    else:        
        # Lectura de datos        
        df_act_dim = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/dim_industry")

        # transformando la dimensión
        df_dim = df_stg_jobposts.select("job_industry1").distinct() \
            .withColumnRenamed("job_industry1", "job_industry") \
            .withColumn("created_date", current_date()) \
            .select("job_industry", "created_date")               
        
        # indentificando incremental de datos
        df_delta_dim = df_dim.join(df_act_dim, df_dim["job_industry"] == df_act_dim["job_industry"],"leftanti") \
            .withColumn("industry_key", expr("uuid()")) \
            .select("industry_key", "job_industry", "created_date")

        df_new_dim = df_act_dim.union(df_delta_dim)

        df_new_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/dim_industry", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/dim_industry") \
            .repartition(2).write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/dim_industry", mode="overwrite")

def load_dim_career_level():
    """    
    proceso de dim_career_level
    """
    config = configparser.ConfigParser()    
    config.read('/home/jovyan/work/tfm-jobs-main/parameters.cfg')
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()
    # Lectura de datos de staging layer
    df_stg_jobposts = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/job_posts")
    
    if loadType=="full":
        # transformando la dimensión
        df_dim = df_stg_jobposts.select("career_level").distinct() \
            .withColumn("career_level_key", expr("uuid()")) \
            .withColumn("created_date", current_date()) \
            .select("career_level_key", "career_level", "created_date")
        df_dim.show(5)
        
        # carga dimensión
        df_dim.repartition(2).write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/dim_career_level", mode="overwrite")
        
    else:        
        # Lectura de datos        
        df_act_dim = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/dim_career_level")

        # transformando la dimensión
        df_dim = df_stg_jobposts.select("career_level").distinct() \
            .withColumn("created_date", current_date()) \
            .select("career_level", "created_date")               
        
        # indentificando incremental de datos
        df_delta_dim = df_dim.join(df_act_dim, df_dim["career_level"] == df_act_dim["career_level"],"leftanti") \
            .withColumn("career_level_key", expr("uuid()")) \
            .select("career_level_key", "career_level", "created_date")

        df_new_dim = df_act_dim.union(df_delta_dim)

        df_new_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/dim_career_level", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/dim_career_level") \
            .repartition(2).write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/dim_career_level", mode="overwrite")

def load_dim_job_requirements():
    """    
    proceso de dim_job_requirements 
    """
    config = configparser.ConfigParser()    
    config.read('/home/jovyan/work/tfm-jobs-main/parameters.cfg')    
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()    
    # Lectura de datos de staging layer
    df_stg_jobposts = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/job_posts")
        
    if loadType=="full":
        #dim_job_requirements      
        # transformando la dimensión
        df_dim = df_stg_jobposts.select("id", "job_requirements").distinct() \
            .withColumn("job_requirements_key", expr("uuid()")) \
            .withColumnRenamed("id", "job_id") \
            .withColumn("created_date", current_date()) \
            .select("job_requirements_key", "job_id", "job_requirements", "created_date") 
        df_dim.show(5)
        
        # carga dimensión
        df_dim.repartition(2).write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/dim_job_requirements", mode="overwrite")
                
    else:        
        # Lectura de datos        
        df_act_dim = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/dim_job_requirements")

        # transformando la dimensión
        df_dim = df_stg_jobposts.select("id", "job_requirements").distinct() \
            .withColumnRenamed("id", "job_id") \
            .withColumn("created_date", current_date()) \
            .select("job_id", "job_requirements", "created_date")              
        
        # indentificando incremental de datos
        df_delta_dim = df_dim.join(df_act_dim, df_dim["job_id"] == df_act_dim["job_id"],"leftanti") \
            .withColumn("job_requirements_key", expr("uuid()")) \
            .select("job_requirements_key", "job_id", "job_requirements", "created_date") 

        df_new_dim = df_act_dim.union(df_delta_dim)

        df_new_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/dim_job_requirements", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/dim_job_requirements") \
            .repartition(2).write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/dim_job_requirements", mode="overwrite")

def load_dim_job_payment():
    """    
    proceso de carga de dim_job_payment
    """
    config = configparser.ConfigParser()    
    config.read('/home/jovyan/work/tfm-jobs-main/parameters.cfg')    
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()    
    # Lectura de datos de staging layer
    df_stg_jobposts = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/job_posts")
        
    if loadType=="full":
        #DIM JOB PAYMENT        
        # transformando la dimensión
        df_dim = df_stg_jobposts.select("payment_period", "currency", "salary_minimum", "salary_maximum").distinct() \
        .withColumn("job_payment_key", expr("uuid()")) \
        .select("job_payment_key", "payment_period", "currency", "salary_minimum", "salary_maximum")\
        .union(spark.createDataFrame([("-1","-","-", 0, 0)],
                                     ["job_payment_key", "payment_period", "currency", "salary_minimum", "salary_maximum"]))\
        .withColumn("created_date", current_date())
        df_dim.show(5)
        
        # carga dimensión
        df_dim.repartition(2).write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/dim_job_payment", mode="overwrite")
                
    else:        
        # Lectura de datos        
        df_act_dim = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/dim_job_payment")

        # transformando la dimensión
        df_dim = df_stg_jobposts.select("payment_period", "currency", "salary_minimum", "salary_maximum").distinct() \
        .withColumn("created_date", current_date()) \
        .select("payment_period", "currency", "salary_minimum", "salary_maximum", "created_date")
        
        # indentificando incremental de datos
        df_delta_dim = df_dim.join(df_act_dim, [df_dim["payment_period"] == df_act_dim["payment_period"],
                                                df_dim["currency"] == df_act_dim["currency"],
                                                df_dim["salary_minimum"] == df_act_dim["salary_minimum"],
                                                df_dim["salary_maximum"] == df_act_dim["salary_maximum"]],"leftanti") \
        .withColumn("job_payment_key", expr("uuid()")) \
        .select("job_payment_key", "payment_period", "currency", "salary_minimum", "salary_maximum", "created_date") 

        df_new_dim = df_act_dim.union(df_delta_dim)

        df_new_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/dim_job_payment", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/dim_job_payment") \
            .repartition(2).write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/dim_job_payment", mode="overwrite")

def load_dim_experience_years():
    """    
    proceso de carga de load_dim_experience_years 
    """
    config = configparser.ConfigParser()    
    config.read('/home/jovyan/work/tfm-jobs-main/parameters.cfg')    
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()    
    # Lectura de datos de staging layer
    df_stg_jobposts = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/job_posts")
        
    if loadType=="full":
        #DIM JOB POST        
        # transformando la dimensión
        df_dim = df_stg_jobposts.select("experience_years").distinct() \
        .withColumn("experience_years_key", expr("uuid()")) \
        .withColumn("created_date", current_date()) \
        .withColumn("min_experience_years", regexp_extract("experience_years", "(\\d{1,2})" , 1 ))\
        .select("experience_years_key", "min_experience_years", "experience_years", "created_date") 
        
        df_dim = df_dim.withColumn("min_experience_years", df_dim["min_experience_years"].cast(t.IntegerType()))
        df_dim.show(5)
        
        # carga dimensión
        df_dim.repartition(2).write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/dim_experience_years", mode="overwrite")
                
    else:        
        # Lectura de datos        
        df_act_dim = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/dim_experience_years")

        # transformando la dimensión
        df_dim = df_stg_jobposts.select("experience_years").distinct() \
        .withColumn("created_date", current_date()) \
        .withColumn("min_experience_years", regexp_extract("experience_years", "(\\d{1,2})" , 1 ))\
        .select("min_experience_years", "experience_years", "created_date")              

        df_dim = df_dim.withColumn("min_experience_years", df_dim["min_experience_years"].cast(t.IntegerType()))
        
        # indentificando incremental de datos
        df_delta_dim = df_dim.join(df_act_dim, [df_dim["min_experience_years"] == df_act_dim["min_experience_years"],
                                                df_dim["experience_years"] == df_act_dim["experience_years"]],"leftanti") \
        .withColumn("experience_years_key", expr("uuid()")) \
        .select("experience_years_key", "min_experience_years", "experience_years", "created_date") 

        df_new_dim = df_act_dim.union(df_delta_dim)

        df_new_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/dim_experience_years", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/dim_experience_years") \
        .repartition(2).write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/dim_experience_years", mode="overwrite")
        
def load_dim_applicant():
    """    
    carga de datos de dim_applicant
    """
    config = configparser.ConfigParser()    
    config.read('/home/jovyan/work/tfm-jobs-main/parameters.cfg')    
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()    
    # Lectura de datos de staging layer
    df_stg_applicant = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/applications")
        
    if loadType=="full":
        #DIM APPLICANT        
        # transformando la dimensión 
        df_dim = df_stg_applicant.select("user_id").distinct() \
            .withColumn("applicant_key", expr("uuid()")) \
            .union(spark.createDataFrame([("-1","-1")],
                                     ["user_id", "applicant_key"]))\
            .withColumn("created_date", current_date()) \
            .select("applicant_key", "user_id", "created_date")
        df_dim.show(5)
        
        # carga dimensión
        df_dim.repartition(2).write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/dim_applicant", mode="overwrite")
        
    else:                
        # Lectura de datos
        df_act_dim = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/dim_applicant")

        # transformando la dimensión
        df_dim = df_stg_benefits.select("user_id").distinct() \
        .withColumn("created_date", current_date()) \
        .select("user_id", "created_date")
        
        # indentificando incremental de datos
        df_delta_dim = df_dim.join(df_act_dim, df_dim["user_id"] == df_act_dim["user_id"],"leftanti") \
            .withColumn("applicant_key", expr("uuid()")) \
            .select("applicant_key", "user_id", "created_date")

        df_new_dim = df_act_dim.union(df_delta_dim)

        df_new_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/dim_applicant", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/dim_applicant") \
            .repartition(2).write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/dim_applicant", mode="overwrite")

def load_dim_skill():
    """    
    carga y transformación de datos de dim_skill
    """
    config = configparser.ConfigParser()    
    config.read('/home/jovyan/work/tfm-jobs-main/parameters.cfg')    
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()    
    # Lectura de datos de staging layer
    df_stg_applicant = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/applicants_skills")
        
    if loadType=="full":
        #DIM SKILLS        
        # transformando la dimensión 
        df_dim = df_stg_applicant.select("name").distinct() \
            .withColumn("skill_key", expr("uuid()")) \
            .withColumnRenamed("name", "skill_name") \
            .withColumn("created_date", current_date()) \
            .select("skill_key", "skill_name", "created_date")
        df_dim.show(5)
        
        # carga dimensión
        df_dim.repartition(2).write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/dim_skill", mode="overwrite")
        
    else:                
        # Lectura de datos        
        df_act_dim = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/dim_skill")

        # transformando la dimensión
        df_dim = df_stg_benefits.select("name").distinct() \
            .withColumnRenamed("name", "skill_name") \
            .withColumn("created_date", current_date()) \
            .select("skill_name", "created_date")
        
        # indentificando incremental de datos
        df_delta_dim = df_dim.join(df_act_dim, df_dim["skill_name"] == df_act_dim["skill_name"],"leftanti") \
            .withColumn("skill_key", expr("uuid()")) \
            .select("skill_key", "skill_name", "created_date")

        df_new_dim = df_act_dim.union(df_delta_dim)

        df_new_dim.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/dim_skill", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/dim_skill") \
            .repartition(2).write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/dim_skill", mode="overwrite")
