"""
Author: Diego Lopez
Last updated: 11/01/2022

This file contains the code to convert the many .log files of the UNSW-NB15
dataset to one .csv file. In a production application, this would not be necessary as we would 
set the zeek monitor to output a specific file type (like JSON), however for this analysis we 
will do it ourselves. 
"""

import pandas as pd
import numpy as np
import zat
import os
import sys
from zat.log_to_dataframe import LogToDataFrame

def main():
    print("INFO [+]: Starting conversion of log files to csv")
    # change this path if necessary
    path                = "../data/raw/UNSW-NB15-Zeek/17-2-2015/"
    bro_df_dns_complete = pd.DataFrame()
    log_to_df           = LogToDataFrame()
    print(f"INFO [+]: Path to data used: {path}")
    for i in os.listdir(path):
        for j in os.listdir(path+i):
            if j == "dns.log":
                temp_bro_df_dns = log_to_df.create_dataframe(path+i+"/"+j)
                bro_df_dns_complete = pd.concat([bro_df_dns_complete, temp_bro_df_dns])
    bro_df_dns_complete.to_csv("../data/processed/bro_dns_complete.csv")
    print("INFO [+]: Completed")
if __name__ == "__main__":
    main()