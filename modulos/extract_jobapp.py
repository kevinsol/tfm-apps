#!/usr/bin/env python
# coding: utf-8
# extract_jobapp.py
# Author: Kevin Solano


from modulos.create_sparkSession import create_spark_session
from pyspark.sql.functions import *
from pyspark.sql import types as t
import configparser

def extract_jobapp():
    """    
    Extraer los datos y crear entidades intermedias
    """
    
    config = configparser.ConfigParser()
    config.read('/home/jovyan/work/tfm-jobs-main/parameters.cfg')
    spark = create_spark_session()
    print("Extracción de datos raw")
    df_jobpost_raw = spark.read.option("header", "true").option("multiline", "true").csv(config['AWS']['S3_BUCKET']+"/raw/jobposts/"+config['PARAM']['DT']+".csv")
    df_app_raw = spark.read.option("header", "true").option("multiline", "true").csv(config['AWS']['S3_BUCKET']+"/raw/applications/"+config['PARAM']['DT']+".csv")  
    df_appskill_raw = spark.read.option("multiline", "true").json(config['AWS']['S3_BUCKET']+"/raw/users/"+config['PARAM']['DT']+".json")
    
    print("Transformación de datos raw a staging")
    # job posts
    df_jobposts = df_jobpost_raw.withColumn("post_new_date", regexp_replace(substring(col("post_date"), 1, 10), "-", ""))\
    .withColumn("post_new_time", regexp_replace(substring(col("post_date"), 12, 5), ":", ""))\
    .withColumn("job_title", regexp_replace(col("job_title"), "[\\r\\n]", " "))\
    .withColumn("job_title", regexp_replace(col("job_title"), "<.*?>", ""))\
    .withColumn("job_title", regexp_replace(col("job_title"), "&amp;", "and"))\
    .withColumn("job_title", trim(upper(col("job_title"))))\
    .withColumn("city", trim(upper(col("city"))))\
    .withColumn("payment_period", upper(col("payment_period")))\
    .withColumn("currency", upper(col("currency")))\
    .withColumn("career_level", upper(col("career_level")))\
    .withColumn("job_category1", trim(upper(col("job_category1"))))\
    .withColumn("job_industry1", trim(upper(col("job_industry1"))))\
    .withColumn("experience_years", regexp_replace(col("experience_years"), "<.*?>", ""))\
    .withColumn("experience_years", trim(upper(col("experience_years"))))\
    .withColumn("job_requirements", regexp_replace(col("job_requirements"), "[\\r\\n]", " "))\
    .withColumn("job_requirements", regexp_replace(col("job_requirements"), "<.*?>", ""))\
    .withColumn("job_requirements", regexp_replace(col("job_requirements"), "&amp;", "and"))\
    .withColumn("job_requirements", regexp_replace(col("job_requirements"), "  ",""))\
    .withColumn("job_requirements", trim(upper(col("job_requirements"))))\
    .withColumn("salary_minimum", df_jobpost_raw["salary_minimum"].cast(t.IntegerType()))\
    .withColumn("salary_maximum", df_jobpost_raw["salary_maximum"].cast(t.IntegerType()))

    
    df_jobposts.show(5)
    
    # aplicaciones
    df_apps = df_app_raw.withColumn("app_new_date", regexp_replace(substring(col("app_date"), 1, 10), "-", ""))\
    .withColumn("app_new_time", regexp_replace(substring(col("app_date"), 12, 5), ":", ""))

    df_apps.show(5)
    
    # aplicantes por competencia
    
    df_appskill = df_appskill_raw.select("user_id",explode("skills")).select("user_id", "col.name", "col.level")
    df_appskill = df_appskill.withColumn("name", trim(upper(col("name"))))\
    .withColumn("level", df_appskill["level"].cast(t.IntegerType()))\
    .withColumnRenamed("level", "skill_level") \
    
    df_appskill.show(5)
    
    # guardar en parquet
    df_jobposts.repartition(4).write.parquet(config['AWS']['S3_BUCKET']+"/staging/job_posts", mode="overwrite")
    df_apps.repartition(4).write.parquet(config['AWS']['S3_BUCKET']+"/staging/applications", mode="overwrite")
    df_appskill.repartition(4).write.parquet(config['AWS']['S3_BUCKET']+"/staging/applicants_skills", mode="overwrite")
    
    print("Capa Staging finalizada")
