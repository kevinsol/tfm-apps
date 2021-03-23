#!/usr/bin/env python
# coding: utf-8
# create_sparkSession.py
# Author: Kevin Solano


from pyspark.sql import SparkSession
import configparser


def create_spark_session():
    """
    Creates and return spark session    
    """

    # Parameter file
    config = configparser.ConfigParser()
    config.read('/home/jovyan/work/tfm-jobs-main/parameters.cfg')

    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", config['AWS']['AWS_AKEYID'])
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config['AWS']['AWS_SAKEY'])

    return spark