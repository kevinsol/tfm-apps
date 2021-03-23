#!/usr/bin/env python
# coding: utf-8
# load_db_redshift.py
# Author: Kevin Solano


from modulos.create_sparkSession import create_spark_session
import configparser
import psycopg2

def load_data_to_redshift():
    """    
    Cargar datos de capa de presentacion de S3 a Redshift
    """
  
    config = configparser.ConfigParser()
    config.read('/home/jovyan/work/tfm-jobs-main/parameters.cfg')    
    spark = create_spark_session()
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['REDSHIFT'].values()))
    cur = conn.cursor()
    S3 = config.get('AWS','S3_BUCKETR')
    ARN = config.get('IAM_ROLE','ARN')
    clean_query = ["TRUNCATE TABLE dim_applicant;",
		"TRUNCATE TABLE dim_job;",
		"TRUNCATE TABLE dim_city;",
		"TRUNCATE TABLE dim_industry;",
		"TRUNCATE TABLE dim_job_payment;",
		"TRUNCATE TABLE dim_job_requirements;",
		"TRUNCATE TABLE dim_skill;",
		"TRUNCATE TABLE dim_career_level;",
		"TRUNCATE TABLE dim_experience_years;",
		"TRUNCATE TABLE bridge_applicant_skills;",
		"TRUNCATE TABLE fact_job_applications;"]

    load_query = [("""  COPY dim_applicant from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/dim_applicant",ARN),
	("""  COPY dim_job from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/dim_job/",ARN),
	("""  COPY dim_city from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/dim_city",ARN),
	("""  COPY dim_industry from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/dim_industry",ARN),
	("""  COPY dim_job_requirements from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/dim_job_requirements",ARN),
	("""  COPY dim_skill from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/dim_skill",ARN),
	("""  COPY dim_career_level from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/dim_career_level",ARN),
	("""  COPY dim_experience_years from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/dim_experience_years",ARN),
	("""  COPY bridge_applicant_skills from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/bridge_applicant_skills",ARN),
	("""  COPY dim_job_payment from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/dim_job_payment",ARN),
	("""  COPY fact_job_applications from '{}'
                               iam_role {}
                               FORMAT AS PARQUET
    """).format(S3+"/presentation/fact_job_applications",ARN)]
	
    # limpieza de tablas
    for query in clean_query:
        cur.execute(query)
        conn.commit()
    print("- Limpieza de tablas finalizada")    

    #carga de tablas en redshift    
    for query in load_query:       
        cur.execute(query)
        conn.commit()
    print("- Carga de tablas en Redshift exitosa")

    conn.close()    
