import os
import glob
import pandas as pd
import csv
import numpy as np

##Combine multiple csv to a single csv
def combined_file(foldername):
    directory = 'T:\\projects\\2019\\EPEC2\\data\\CSV-output\\redback\\'
    os.chdir(directory)

    extension = 'csv'
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

    ##Combine all files in the list
    combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ], sort=False)
    ##Export to csv
    csv_name = '\\' + foldername + '.csv'
    combined_csv.to_csv("T:\\projects\\2019\\EPEC2\\data\\CSV-output\\redback" + csv_name, index = None, header = True)

    return foldername
    
folder_name = combined_file("2018Winter6-8")



