#!/usr/bin/python

import sys
import pandas as pd
from pandas.io.sql import SQLTable
from pandas import DataFrame
import logging
import yaml
from urllib.parse import quote
import sqlalchemy as sa
import os
from sqlalchemy import text, Engine, Connection, Table, TextClause
from src.telegram_bot import enviar_mensaje
import numpy as np
import asyncio


act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir = os.path.join(act_dir, '..')
sys.path.append(proyect_dir)

params = {'interval': 1}

logging.basicConfig(
    level=logging.INFO,
    filename=(log_dir := os.path.join(proyect_dir, 'log', 'logs_main.log')),
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt='%d-%b-%y %H:%M:%S'
)


def get_engine(username: str, password: str, host: str, database: str, port: str = 3306, *_) -> Engine:
    return sa.create_engine(f"mysql+pymysql://{username}:{quote(password)}@{host}:{port}/{database}")


with open(yml_credentials_dir := os.path.join(proyect_dir, 'config', 'credentials.yml'
                                              ), 'r') as file_yml:

    try:
        config = yaml.safe_load(file_yml)
        source1, source2, source3, source4 = config['source1'], config['source2'], config['source3'], config['source4']
    except yaml.YAMLError as error_log_1:
        logging.error(str(error_log_1), exc_info=True)


def engine_1() -> Connection:
    return get_engine(**source1).connect()


def engine_2() -> Connection:
    return get_engine(**source2).connect()


def engine_3() -> Connection:
    return get_engine(**source3).connect()


def engine_4() -> Connection:
    return get_engine(**source4).connect()


def import_query_date(sql, parameters: dict) -> TextClause:

    with open(sql, 'r') as file_sql:

        try:
            querys = file_sql.read()
            query = text(querys).bindparams(**parameters)
            return query

        except yaml.YAMLError as error_log_2:
            logging.error(str(error_log_2), exc_info=True)


def import_query(sql) -> TextClause:

    with open(sql, 'r') as file_sql:

        try:
            querys = file_sql.read()
            query = text(querys)
            return query

        except yaml.YAMLError as error_log_3:
            logging.error(str(error_log_3), exc_info=True)


def extract(query: text, conn: Engine | Connection) -> DataFrame:

    with conn as con:
        df = pd.read_sql_query(query, con)
        logging.info(f'se leen {len(df)}')    
        return df


def to_sql_replace(table: SQLTable, con: Engine | Connection, keys: list[str], data_iter):

    satable: Table = table.table
    ckeys = list(map(lambda s: s.replace(' ', '_'), keys))
    data = [dict(zip(ckeys, row)) for row in data_iter]
    values = ', '.join(f':{nm}' for nm in ckeys)
    stmt = f"REPLACE INTO {satable.name} VALUES ({values})"

    con.execute(text(stmt), data)


def load(name: str, conn: Engine | Connection, action: str, index: bool, df: pd.DataFrame):

    with conn as con:

        try:
            df.to_sql(name=name, con=con, if_exists=action, index=index, method=to_sql_replace)
            logging.info(f'Se cargan {len(df)} datos')
            asyncio.run(enviar_mensaje(f'Se cargan {len(df)} datos'))   
            print(f'Se cargan {len(df)} datos')

        except KeyError as error_log:

            logging.error(str(error_log), exc_info=True)


def transform() -> DataFrame:    
    df_extend = extract(import_query_date(os.path.join(
        proyect_dir, 'sql', 'df_extend.sql'), params), engine_1())
    df_headcount = extract(import_query(os.path.join(proyect_dir, 'sql', 'df_headcount.sql')
                                        ), engine_2())      
    df_join = pd.merge(df_extend, df_headcount, left_on='user', right_on='Documento', how='left')    
    df_join['gestion'] = np.where(df_join['status'].isin(['RETEN']), 'RETENIDO', 'NO-RETENIDO')   
    df_join['proceso'] = 'R-PRIORITARIAS'
    df_join['aliado'] = 'COS-BOGOTA'
    df_join['segmento'] = 'HOGAR-RECUPERACION'
    df_join['subproceso'] = 'TERCER-ANILLO'   
    df_join['call_date'] = pd.to_datetime(df_join['call_date'])
    df_join['dia'] = df_join['call_date'].dt.day
    df_join['mes'] = df_join['call_date'].dt.month
    df_join['ano'] = df_join['call_date'].dt.year 
    df_join['fecha'] = df_join['dia'].astype(str)+'-'+df_join['mes'].astype(str)+'-'+df_join['ano'].astype(str)   
    df_join['hora'] = df_join['call_date'].astype(str).apply(lambda x: x[-8:].replace(':', '-'))   
    df_join['call_date'] = df_join['call_date'].astype(str)   
    df_join = df_join.drop(['dia', 'mes', 'ano'], axis=1).rename(
                                                        columns={'length_in_sec': 'TMO'})   
    df_recording_log = extract(import_query_date(
        os.path.join(proyect_dir, 'sql', 'df_recording_log.sql'), params), engine_3())      
    df_recording_log['lead_id1'] = df_recording_log['lead_id1'].astype(str)   
    df_join = pd.merge(df_join, df_recording_log, left_on=['lead_id'], right_on=['lead_id1'], how='left') 
    df_join = df_join.drop(columns=['lead_id1'])  
    df_join['tipo_gestion'] = 'outbound'
    return df_join
    
            
