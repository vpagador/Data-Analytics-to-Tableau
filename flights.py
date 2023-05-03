import numpy as np
import os
import pandas as pd


cast_columns = {'Year': np.dtype('int32'),'Month':np.dtype('int32'), 'DayofMonth':np.dtype('int32'),
                'DayOfWeek':np.dtype('int32'),'DepTime':np.dtype('int32'),'CRSDepTime':np.dtype('int32'),
                'ArrTime':np.dtype('int32'),'CRSArrTime':np.dtype('int32'),'UniqueCarrier':'category',
                'FlightNum':np.dtype('int32'),'ActualElapsedTime':np.dtype('int32'), 'CRSElapsedTime':np.dtype('int32'),
                'ArrDelay':np.dtype('int32'), 'DepDelay':np.dtype('int32'), 'Origin':'category','Dest':'category', 
                'Distance':np.dtype('int32'),'TaxiIn':np.dtype('int32'),'TaxiOut':np.dtype('int32'),'Cancelled':np.dtype('int32'),
                'Diverted':np.dtype('int32'),'TailNum':np.dtype('int32'),'AirTime':np.dtype('int32')
                }


class Flights:

    def __init__(self, folder = "data-analytics-files"):
        self.folder = folder

    def list_files(self) -> list:
        files_in_folder = os.listdir(self.folder)
        return files_in_folder
    
    def read_file(self, file:str): 
        df = pd.read_csv(f"{self.folder}/{file}")
        return df

    def clear_null_values(self,df):
        df = df.dropna(axis='columns',how='all')
        df = df.fillna(0)
        return df

    def concat_dataframes(self,list_of_df):
        master_df = pd.concat(list_of_df)
        return master_df

if __name__=='__main__':
    flights = Flights()
    list_of_files = flights.list_files()
    list_of_df = []
    for file in list_of_files:
        df = flights.read_file(file)
        if file not in ['1995','1996']:
            df['TaxiIn'] =0
            df['TaxiOut']=0
            df['TailNum']=0
            df['AirTime']=0
        clean_df = flights.clear_null_values(df)
        clean_df.astype(cast_columns)    
        list_of_df.append(clean_df)
        print(len(list_of_df))
        print('success')
    
    master_df = flights.concat_dataframes(list_of_df)
    print('got a master df')
    master_df.to_csv('master_flights.csv', chunksize=1000)

    



        


        

