#!/usr/bin/env python
# coding: utf-8
# load_bridges.py
# Author: Kevin Solano


from modulos.create_sparkSession import create_spark_session
from pyspark.sql.functions import *
from pyspark.sql import types as t
import configparser
      
def load_bridge_applicant_skills():
    """    
    carga de tabla de puente de datos entre aplicantes y competencias
    """
    config = configparser.ConfigParser()
    config.read('/home/jovyan/work/tfm-jobs-main/parameters.cfg')
    
    loadType = config.get('PARAM', 'LOADTYPE')

    spark = create_spark_session()
    # Lectura de datos de staging 
    df_stg_applicant = spark.read.parquet(config['AWS']['S3_BUCKET']+"/staging/applicants_skills")
    
    # Lectura de dimensiones
    df_dim_app = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/dim_applicant")
    df_dim_ski = spark.read.parquet(config['AWS']['S3_BUCKET']+"/presentation/dim_skill")
    
    if loadType=="full":
        # Transformación de la dimensión
        df_stg_app = df_stg_applicant.join(df_dim_app,
                                           [df_stg_applicant["user_id"] == df_dim_app["user_id"]], "inner")\
        .select("applicant_key", "name", "skill_level")
        
        df_stg_bridge = df_stg_app.join(df_dim_ski,df_stg_app["name"]==df_dim_ski["skill_name"],"inner")\
        .select("applicant_key", "skill_key", "skill_level")\
        .withColumn("created_date", current_date())
        
        df_stg_bridge.show(5)
        
        # carga de datos
        df_stg_bridge.repartition(2).write.parquet(config['AWS']['S3_BUCKET'] + "/presentation/bridge_applicant_skills",
                                     mode="overwrite")
        
    else:
        # Lectura de datos actuales
        df_act_bridge = spark.read.parquet(config['AWS']['S3_BUCKET']+ "/presentation/bridge_applicant_skills")
        
        # Transformación de la dimensión
        df_stg_app = df_stg_applicant.join(df_dim_app,
                                           [df_stg_applicant["user_id"] == df_dim_app["user_id"]], "inner")\
        .select("applicant_key", "name", "skill_level")\
        .withColumn("created_date", current_date())
        
        df_stg_bridge = df_stg_app.join(df_dim_ski,df_stg_app["name"]==df_dim_ski["skill_name"],"inner")\
        .select("applicant_key", "skill_key", "skill_level")
        
        # indentificando incremental
        df_delta_bridge = df_stg_bridge.join(df_act_bridge,
                                             [df_stg_bridge["applicant_key"] == df_act_bridge["applicant_key"],
                                              df_stg_bridge["skill_key"] == df_act_bridge["skill_key"]],
                                             "leftanti") \
        .select("applicant_key", "skill_key", "skill_level", "created_date")

        df_new_bridge = df_act_bridge.union(df_delta_bridge)

        df_new_bridge.write.parquet(config['AWS']['S3_BUCKET'] + "/tmp/bridge_applicant_skills", mode="overwrite")

        spark.read.parquet(config['AWS']['S3_BUCKET']+ "/tmp/bridge_applicant_skills") \
            .repartition(2).write.parquet(config['AWS']['S3_BUCKET']+ "/presentation/bridge_applicant_skills", mode="overwrite")
        
