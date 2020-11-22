# -*- coding: utf-8 -*-
"""
Created on Sun Nov 22 15:50:48 2020

@author: QUANTMOON
"""

from whitehole.decryptor import Decryptor

repo_path = '' ## Insert the stock repository path here
symbol = '' ## Insert a stock from repo_path

dc = Decryptor(repo_path,
                 symbol,
                 date = None, ## If just 1 day is needed
                 start_date = '2020-08-03', 
                 end_date = '2020-08-05', 
                 save = True, 
                 dataframe = True,
                 storage_path = '', ## The path where the information will be saved
                 global_result = False)

df = dc.run_decryptor()

print(df)

