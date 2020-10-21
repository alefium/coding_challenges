# -*- coding: utf-8 -*-
"""
Created on Tue Oct 20 2020

@author: Alessio F.
"""

import os

from python_challenge import get_data, ingest_data


def main():
    password = ''
    
    path = os.path.join('C:/', 'Users', 'Home', 'Documents', 'DE', 'source_data')
    file_name = 'orders'
    api_key = ''
    
    df = get_data(path, api_key, file_name)
    ingest_data(password, df)
    
    print('Done.')


if __name__ == '__main__':
    main() 
    
    
    


