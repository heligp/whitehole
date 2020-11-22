# -*- coding: utf-8 -*-
"""
Created on Sat Aug 15 14:48:14 2020

@author: QUANTMOON
"""

import sys
import numpy as np
import zarr
import pandas as pd
from dateutil import parser
from datetime import datetime
import pandas_market_calendars as mcal


class BaseDecryptor(object):
    """
    General Decryptor Functions
    
    1. trading_calendar(): 
        - upload trading days as datetimes.
    2. extract_date_available_marker(): 
        - match trading NYSE days with input days.
    3. range_time():
        - makes range-time based on 'str' hours input.
    4. read_zarr():
        - get np.array infomration from zarr files.
    """
    def trading_calendar():
        """
        Gets trading days based on NYSE Calendar.
        """
        nyse = mcal.get_calendar('NYSE')
        early = nyse.schedule(start_date='2015-01-01', end_date='2021-04-28')
        dts = list(early.index.date)
        
        #transform as datetime.date() each string date
        return dts


    def extract_date_available_market(self, 
                                      start_, 
                                      end_, 
                                      trd_cal_=trading_calendar()):
        """
        Match NYSE trading calendar days with input days.
        """
        #define start and endDate as datetime
        startDate=datetime.strptime(start_,'%Y-%m-%d')
        endDate=datetime.strptime(end_,'%Y-%m-%d')
        
        #check if both are the same dates
        if startDate == endDate:
            
            #assign single startdate as date() only
            list_pre = [startDate.date()]
            
            #find min nearest date to set a benchmark 
            date = min(
                trd_cal_, key= lambda x: abs(x - list_pre[0])
            )
            
            #check if nearest is the same as input date
            if date == list_pre[0]:
                idx = [trd_cal_.index(date)]
                return [trd_cal_[idx[0]].strftime('%Y-%m-%d')]         
            #if not is the same
            else:
                print("No trading days at {}".format(
                    startDate.date())
                     )
                sys.exit()
        #if input dates are different 
        else:
            #check nearest date to start date
            date = min(
                trd_cal_, key=lambda x: abs(x - startDate.date())
            )
            idx_1 = trd_cal_.index(date)
            
            #check nearest date to end date
            date = min(
                trd_cal_, key=lambda x: abs(x - endDate.date())
            )
            idx_2 = trd_cal_.index(date)
            
            #make range of dates
            resulted_dates_range = trd_cal_[idx_1:idx_2+1]
            
            #check if range of dates is less than 1 (no dates)
            if len(resulted_dates_range)<1:
                print("No trading days in {} to {}".format(
                    startDate.date(), 
                    endDate.date())
                     )
                sys.exit()
                
            #if there is at least one trading day
            else:
                return [result_date_.strftime('%Y-%m-%d') 
                        for result_date_ in resulted_dates_range]  

    def range_time(self, date, init, last):
        """
        Makes range-time based on 'str' hours input.
        """
        #avoid space issues
        init = init.replace(" ","")
        last = last.replace(" ","")
        
        #check if both are the same
        if init == last:
            print("No range among time bounds")
            sys.exit()

        
        #transform 'init' string into datetime
        _initialization = datetime.timestamp(
                parser.parse(
                        date + " " + init
                        )
                ) * (10**3)
        
        #transform 'last' string into datetime
        _finalization = datetime.timestamp(
                parser.parse(
                        date + " " + last
                        )
                ) * (10**3)
        
        #return tuple of information
        return (_initialization, _finalization)
    
    def open_zarr(self, stock,path):         
        new_path = path+stock+".zarr"
        zarrds = zarr.open_group(new_path)
        return zarrds
    
            
    #Funciones para generar el df con precio y volumen
    def load_data(self, symbol,path,dates):
        lista = [] 
        zarrds = self.open_zarr(symbol,path)
        for date in dates:
            #print(date)
            prices,volume,timestamp = self.get_arrs( zarrds,date)
            X = self.construct_df(prices,volume,timestamp)
            new_ts = [datetime.fromtimestamp(i) for i in X['ts']/1000]
            X['ts'] = new_ts#ts_idx
            X.set_index('ts',inplace=True)
            lista.append(X)
        result = pd.concat(lista)
        return result
    
    #Funciones para generar el df con precio y volumen
    def load_data_day(self, symbol,path,date):
        zarrds = self.open_zarr(symbol,path)
        #print(date)
        prices,volume,timestamp = self.get_arrs( zarrds,date)
        X = self.construct_df(prices,volume,timestamp)
        new_ts = [datetime.fromtimestamp(i) for i in X['ts']/1000]
        X['ts'] = new_ts#ts_idx
        X.set_index('ts',inplace=True)
        
        return X
    
    #segmentation based on time init and time last
    def get_arrs(self, zarrds,date_):
        arr = np.array(zarrds.date)
        idx = np.where(arr == date_)[0][0]
        prices =  zarrds.value[idx]
        prices = prices[prices>0]
        volume = zarrds.vol[idx]
        volume = volume[:len(prices)]
        timestamp = zarrds.timestamp[idx]
        timestamp = timestamp[:len(prices)]
        return prices,volume, timestamp

    
    #construction of final dataframe        
    def construct_df(self, prices,volume,timestamp):
        df = pd.DataFrame({
                'ts':timestamp,
                'price':prices,
                'vol':volume,
                })
        return df
    
    def saves(self, result_df, date_):
        
        nameFile = self.symbol+"_"+date_
        
        result_df.to_csv(self.storage_path+nameFile+'.txt')   
        print("{} {} saved".format(self.symbol, date_))
        #save and return None
        return None
        


class Decryptor(BaseDecryptor):
    """
    Main Decryptor Class.
    
    Summarize: 
        Transform xarray.Dataset information into a useful format.
    Results:
        Generates a numpy, pd.DataFrame or a .txt format to save.
    
    Parameters
    ----------
    * repo_path: path directory where the zarr-files are located
    * symbol: string of stock symbol ID (e.g., Apple INC = AAPL)
    * date: string of single date as "YYYY-MM-DD"
    * start_date: initial string date as "YYYY-MM-DD" 
    * end_date: final string date as "YYY-MM-DD"
    * save: bool to access saving data as .txt files 
    * dataframe: bool that transforms output in a dataframe
    * storage_path: path directory where .txt files will be saved
    
    Output
    ------
    Output 1:
        *np.array() with shape(#ticks, 3).
        * pd.Dataframe with shape(#ticks,3). 
          Columns: 'ts','price','vol'
        * .txt file in path directory as pd.Dataframe structure.
    """
    def __init__(self, 
                 repo_path,
                 symbol,
                 date = None, 
                 start_date = None, 
                 end_date = None, 
                 save = False, 
                 dataframe = False,
                 storage_path = None,
                 global_result = False):
        
        #defining parameters
        self.repo_path = repo_path
        self.symbol = symbol
        self.date = date
        self.start_date = start_date
        self.end_date = end_date
        self.save = save
        self.dataframe = dataframe
        self.storage_path = storage_path
        self.global_result = global_result
        
        #check if full_day is requested
        
        
        #check if single 'date' is requested
        if self.date is not None:
            self.start_date = self.date
            self.end_date = self.date
        
        #check if 'save' request is active without path for storage
        if self.save and self.storage_path is None:
            print("Define 'storage_path' direction to save data.")
            sys.exit()
            
    def run_decryptor(self):
        """
        Decryptor Function
        
        Evaluates informations requirements.
        
        Select final result format:
            - 'save = True': .txt file in 'storage_path'
            - 'save = False':
                - 'dataframe = True': returns pd.DataFrame
                - 'dataframe = False': returns np.array(n_days, 3)
        """
        #get range of dates
        range_dates = self.extract_date_available_market(
            self.start_date,
            self.end_date
        )
        
        
        
        if self.dataframe == False:
            zarrds = self.open_zarr(self.symbol,self.repo_path)
            return [self.get_arrs(zarrds,date) for date in range_dates]
                
        
        if self.global_result == True:
            dates = str(range_dates[0]) + "_" + str(range_dates[-1])
            #iteration over dates
            
            result_ = self.load_data(self.symbol,
                                    self.repo_path,
                                    range_dates)
           
            

            #check if save is requested
            if self.save: 
                self.saves(result_,dates)
            
            return result_
            
        elif self.global_result == False:
            for date in range_dates:
                result_ = self.load_data_day(self.symbol,
                                                    self.repo_path,
                                                    date)
                if self.save: 
                    self.saves(result_,date)
