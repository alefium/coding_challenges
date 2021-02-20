# -*- coding: utf-8 -*-
"""
Created on Tue Oct 20 2020

@author: Alessio F.
"""

import logging
import urllib

from io import StringIO

import numpy as np
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import BIGINT, BOOLEAN, CHAR, DOUBLE_PRECISION, INTEGER, TEXT, TIMESTAMP 
from sqlalchemy.engine import Engine


def sql_engine(password, database: str='prova', user: str='postgres') -> Engine:
    ''' Returns a Sqlalchemy engine. '''
    
    return create_engine(f'postgresql+psycopg2://{user}:{password}@localhost/{database}')


def create_order_table(password: str) -> None:
    ''' Creates the order table in a postgreSQL database. '''
    
    with sql_engine(password).connect() as con:  
        con.execute('''create table dna_test.order (
                                    id integer not null generated always as identity,
                                    order_id bigint,
                                    order_date timestamp,
                                    user_id int,
                                    zipcode int,
                                    total double precision,
                                    item_count int,
                                    lat double precision,
                                    lng double precision,
                                    city text,
                                    state text,
                                    timezone_identifier text,
                                    timezone_abbr char(3),
                                    utc_offset_sec int,
                                    is_dst boolean,
                                    area_codes text,
                                    timerange_transaction tstzrange default tstzrange(current_timestamp, 
                                                                             null, 
                                                                             '[)'),
                                    
                                    constraint cons_dna_test_order_pk primary key (id)
                                );
        
                        create index dna_test_order_order_id_first
                            on dna_test.order (order_id, timerange_transaction);
                            
                        create index dna_test_order_user_id_first
                            on dna_test.order (user_id, order_date, timerange_transaction);
                        ''')
                        

def get_location(zip_codes: set, api_key: str) -> pd.DataFrame:
    ''' Get location details given a zip code using www.zipcodeapi.com '''
    
    # Building the string of zip codes.
    zip_codes_str = ','.join(str(zip_code) for zip_code in zip_codes)
    
    # Trying first to run a multiple request.
    try:
        location_bytes = urllib.request.urlopen(f'https://www.zipcodeapi.com/rest/{api_key}/multi-info.csv/{zip_codes_str}/degrees').read()
        location_str = str(location_bytes,'utf-8')
        location_data = StringIO(location_str) 
        location_df = pd.read_csv(location_data)
    
    except urllib.error.HTTPError as e:
        # In case of error in the multiple request, we run single requests.
        logging.error(f'Multi-info error: {e}')
        location_df = pd.DataFrame(columns=['zip_code','lat','lng','city','state','timezone_identifier','timezone_abbr','utc_offset_sec','is_dst','area_codes'])
        for zip_code in zip_codes:
            try:
                single_location_bytes = urllib.request.urlopen(f'https://www.zipcodeapi.com/rest/{api_key}/info.csv/{str(zip_code)}/degrees').read()
            except urllib.error.HTTPError as e:
                logging.error(f'Multi-info error for zip_code {zip_code}: {e}') 
                continue
                
            single_location_str = str(single_location_bytes,'utf-8')
            single_location_data = StringIO(single_location_str) 
            single_location_df = pd.read_csv(single_location_data)
            location_df = pd.concat([location_df, single_location_df])

    return location_df


def get_data(path: str, api_key: str, file_name: str='orders') -> pd.DataFrame:
    ''' Gets data from file and retrieves the location from the zip code. '''
    
    new_df = pd.read_excel(f'{path}\\{file_name}.xlsx')
    
    # Taking a set of zip codes in order to avoid multiple requests for the same code.
    zip_codes = set(new_df['zipcode'].unique())
    
    # Getting the location data.
    location_df = get_location(zip_codes, api_key)
    
    # Merging and polishing the data retrieved.
    complete_df = pd.merge(new_df, location_df, how='left', left_on='zipcode', right_on='zip_code')
    complete_df.drop(columns='zip_code', inplace=True)
    complete_df['is_dst'] = np.where(complete_df['is_dst'] == 'T', True, False)
    
    return complete_df


def ingest_data(password: str, data: pd.DataFrame, table_name: str='order') -> None:
    ''' Ingests the retrieved data into the database. '''
    
    with sql_engine(password).connect() as con:  
        # Creating a service table to store temporarily the data in order to perform additional checks.
        data.to_sql(f'{table_name}_to_ingest', con=con, schema='dna_test', if_exists='replace', index=False,
                    dtype={'order_id': BIGINT,
                           'order_date': TIMESTAMP,
                           'user_id': INTEGER,
                           'zipcode': INTEGER,
                           'total': DOUBLE_PRECISION,
                           'item_count': INTEGER,
                           'lat': DOUBLE_PRECISION,
                           'lng': DOUBLE_PRECISION,
                           'city': TEXT,
                           'state': TEXT,
                           'timezone_identifier': TEXT,
                           'timezone_abbr': CHAR(3),
                           'utc_offset_sec': INTEGER,
                           'is_dst': BOOLEAN,
                           'area_codes': TEXT})
        
        # Ingesting the data into the proper table avoiding to store records we already have.
        # We assume here that the order_id is unique.
        # This version is the minimum we should do to store data avoiding repetitions, but we can better
        # use the field timerange_transaction to create a proper "point in time" table.
        con.execute(f'''insert into dna_test.{table_name} 
                        (order_id, order_date, user_id, zipcode, total, item_count,
                         lat, lng, city, state, timezone_identifier, timezone_abbr,
                         utc_offset_sec, is_dst, area_codes)
                        select *
                        from dna_test.{table_name}_to_ingest bb
                        where not exists (
                            select *
                            from dna_test.{table_name} cc
                            where cc.order_id = bb.order_id
                                         )
                    ''')
        


















