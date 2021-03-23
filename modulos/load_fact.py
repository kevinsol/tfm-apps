#!/usr/bin/env python
# coding: utf-8
# load_fact.py
# Author: Kevin Solano


from modulos.create_sparkSession import create_spark_session
from pyspark.sql.functions import *
from pyspark.sql import types as t
import configparser
from pyspark.sql import Window
      
def load_fact():
    """    
    carga de tabla de hechos
    """
    config = configparser.ConfigParser()
    config.read('/home/jovyan/work/tfm-jobs-main/parameters.cfg')
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()
    # Lectura de datos de staging 
    df_stg_jobpost = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/job_posts")
    df_stg_applicant = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/applications")\
    .withColumnRenamed("user_id", "userid")
    
    # Lectura de dimensiones
    df_dim_app = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/dim_applicant")
    df_dim_clv = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/dim_career_level")
    df_dim_cty = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/dim_city")
    df_dim_exy = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/dim_experience_years")
    df_dim_ind = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/dim_industry")
    df_dim_jpy = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/dim_job_payment") 
    df_dim_jrq = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/dim_job_requirements")
    df_dim_job = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/dim_job")
    
    # Obtener llaves de jobsposts
        
    df_jobpost = df_stg_jobpost.join(df_dim_clv, df_stg_jobpost["career_level"]==df_dim_clv["career_level"], "left")\
    .join(df_dim_cty, df_stg_jobpost["city"]==df_dim_cty["city"], "left")\
    .join(df_dim_exy, df_stg_jobpost["experience_years"]==df_dim_exy["experience_years"], "left")\
    .join(df_dim_ind, df_stg_jobpost["job_industry1"]==df_dim_ind["job_industry"], "left")\
    .join(df_dim_jpy, [df_stg_jobpost["payment_period"]==df_dim_jpy["payment_period"],
                      df_stg_jobpost["currency"]==df_dim_jpy["currency"],
                      df_stg_jobpost["salary_minimum"]==df_dim_jpy["salary_minimum"],
                      df_stg_jobpost["salary_maximum"]==df_dim_jpy["salary_maximum"]], "left")\
    .join(df_dim_jrq, df_stg_jobpost["id"]==df_dim_jrq["job_id"], "left")\
    .join(df_dim_job, [df_stg_jobpost["job_title"] == df_dim_job["job_title"], 
                       df_stg_jobpost["job_category1"] == df_dim_job["job_category"]], "left")\
    .withColumn("Views", df_stg_jobpost["Views"].cast(t.IntegerType()))\
    .withColumn("num_vacancies", df_stg_jobpost["num_vacancies"].cast(t.IntegerType()))\
    .withColumnRenamed("Views", "job_views")\
    .withColumnRenamed("num_vacancies", "job_num_vacancies")\
    .withColumnRenamed("post_new_date", "job_post_date_key")\
    .withColumnRenamed("post_new_time", "job_post_time_key")\
    .withColumnRenamed("job_id", "job_id_dd")\
    .select("career_level_key", "city_key", "experience_years_key", "industry_key", "job_payment_key", "job_requirements_key", "job_key", "job_post_date_key", "job_post_time_key", "job_id_dd", "job_num_vacancies", "post_date", "job_views")
    
    # Obtener llaves de applications y generar tabla fact    
    df_fac_apps = df_stg_applicant.join(df_dim_app, df_stg_applicant["userid"]==df_dim_app["user_id"], "inner")\
    .withColumn("rownumber", row_number().over(Window.partitionBy("job_id").orderBy("id")))\
    .withColumn("total_jobs", when(col("rownumber") == 1, 1).otherwise(0))\
    .withColumn("total_applicants", lit(1))\
    .withColumnRenamed("id", "application_id_dd")\
    .withColumnRenamed("app_new_date", "application_date_key")\
    .withColumnRenamed("app_new_time", "application_time_key")\
    .join(df_jobpost, df_stg_applicant["job_id"] ==  df_jobpost["job_id_dd"], "right")\
    .withColumn("days_between_post_app",datediff(col("app_date"), col("post_date")))\
    .withColumn("created_date", current_date()) \
    .fillna({'job_payment_key': '-1'}) \
    .select("application_id_dd", "application_date_key", "application_time_key", "job_post_date_key", "job_post_time_key", "job_key", "job_id_dd", "industry_key", "city_key",  "job_payment_key", "career_level_key", "experience_years_key", "applicant_key", "job_requirements_key", "job_views", "job_num_vacancies", "total_jobs", "total_applicants", "days_between_post_app", "created_date")
      
    df_fac_apps = df_fac_apps.fillna({'applicant_key': '-1','application_id_dd': '-1','application_date_key': '-1','application_time_key': '-1', 'days_between_post_app': -1, 'total_applicants':0, 'total_jobs':1})
    df_fac_apps.show(5)
    
    raw_count = df_stg_applicant.count()
    fact_count = df_fac_apps.filter("total_applicants = 1").count()
    jobs_without_app_count = df_fac_apps.filter("total_applicants = 0").count()

    if loadType=="full":
        
        # carga de datos
        df_fac_apps.repartition(4).write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/fact_job_applications",
                                     mode="overwrite")
        
    else:
        # Lectura de datos actuales
        df_act_fact = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/fact_job_applications")
        
        # indentificando incremental
        df_delta_fact = df_fac_apps.join(df_act_fact,
                                         df_fac_apps["application_id_dd"] == df_act_fact["application_id_dd"],
                                         "leftanti") \

        df_new_fact = df_act_fact.union(df_delta_fact)

        df_new_fact.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/fact_job_applications", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/fact_job_applications") \
            .write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/fact_job_applications", mode="overwrite")
        
    print("--------------------------------------------------------------")
    print("CONTROL DE CALIDAD")
    print("--------------------------------------------------------------")
    print("Aplicaciones raw data: "+str(raw_count))
    print("Aplicaciones fact table: "+str(fact_count))
    print("Registros duplicados: "+str(fact_count-raw_count))
    print("Consistencia de datos: "+str((raw_count/fact_count)*100)+"%")
    print("Jobs sin aplicaciones: "+str(jobs_without_app_count))
    print("--------------------------------------------------------------")
    print(" ")
        
