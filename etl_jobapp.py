#!/usr/bin/env python
# coding: utf-8
# etl_jobapp.py
# Author: Kevin Solano
# Proceso ETL principal

from modulos.extract_jobapp import extract_jobapp
from modulos.load_dims import *
from modulos.load_bridges import *
from modulos.load_fact import *
from modulos.load_db_redshift import *
import configparser

def main():
    """    
    Proceso ETL para cargar modelo de aplicaciones a ofertas de empleo
    """
    # Parameter file
    config = configparser.ConfigParser()
    config.read('/home/jovyan/work/tfm-jobs-main/parameters.cfg')

    startstep = int(config.get('PARAM','STARTSTEP'))
    endstep = int(config.get('PARAM','ENDSTEP'))
        
    step = 1
    if step >= startstep and step <= endstep:
        print("1. extracción de datos y limpieza de datos en staging layer")
        extract_jobapp()
        
    step = step +1
    if step >= startstep and step <= endstep:
        print("2. Transformación y actualización de dim_job")
        load_dim_job()
        
    step = step +1
    if step >= startstep and step <= endstep:
        print("3. Transformación y actualización de dim_industry")
        load_dim_industry()
            
    step = step +1
    if step >= startstep and step <= endstep:
        print("4. Transformación y actualización de dim_city")
        load_dim_city()
            
    step = step +1
    if step >= startstep and step <= endstep:
        print("5. Transformación y actualización de dim_career_level")
        load_dim_career_level()
        
    step = step +1
    if step >= startstep and step <= endstep:
        print("6. Transformación y actualización de dim_job_requirements")
        load_dim_job_requirements()    
        
    step = step +1
    if step >= startstep and step <= endstep:
        print("7. Transformación y actualización de dim_job_payment")
        load_dim_job_payment()
            
    step = step +1
    if step >= startstep and step <= endstep:
        print("8. Transformación y actualización de dim_applicant")
        load_dim_applicant()
        
    step = step +1
    if step >= startstep and step <= endstep:
        print("9. Transformación y actualización de dim_skill")
        load_dim_skill()
        
    step = step +1
    if step >= startstep and step <= endstep:
        print("10. Transformación y actualización de dim_experience_years")
        load_dim_experience_years()
            
    step = step +1
    if step >= startstep and step <= endstep:
        print("11. Transformación y actualización de bridge_applicant_skills")
        load_bridge_applicant_skills()
    
    step = step +1
    if step >= startstep and step <= endstep:
        print("12. Transformación y actualización de fact table")
        load_fact()
        
    step = step +1
    if step >= startstep and step <= endstep:
        print("13. Carga de modelo dimensional a redshift")        
        load_data_to_redshift()


if __name__ == "__main__":
    main()
