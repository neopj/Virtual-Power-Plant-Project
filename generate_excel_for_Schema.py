import os
import glob
import pandas as pd
import csv
import numpy as np

##Function to produce the statistical value of differeent variables for Data Schema
def summary():
    #Path is defined
    directory = 'T:\\projects\\2019\\EPEC2\\data\\CSV-output\\'
    os.chdir(directory)
    
    extension = 'csv'
    #Gets all the csv files 
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
    #print(all_filenames)
    
    for files in all_filenames:
        #Get each csv files
        file = directory + files
        read_file = pd.read_csv(file) 
        #print(type(read_file))
        df = pd.DataFrame()
        vtype = read_file.dtypes
        #print(type(vtype))
        nempty = pd.DataFrame.count(read_file)    
        unique = read_file.nunique()
        count = len(read_file) 
        empty = count - nempty
        describe = read_file.describe().T    
        measurer = np.vectorize(len)
        #minL = measurer(read_file.values.astype(str)).min(axis=0)
        maxL = measurer(read_file.values.astype(str)).max(axis=0)
                   
        df['Type'] = vtype
        df['Count'] = count    
        df['Unique'] = unique
        df['Non Empty'] = nempty
        df['Empty'] = empty
        df['Min'] = describe['min']
        df['Max'] = describe['max']
        df['Mean'] = round(describe['mean'], 3)
        df['Std Dev'] = round(describe['std'], 3)
        df['Min Length'] = minL 
       
        
        name = files.split('.')[0]
        #Saves as a csv file
        df.to_excel(r'T:\\projects\\2019\\EPEC2\\data\\CSV-output\\DataSchema_Output\\' + name + '.xlsx')
    
    return df

print(summary())


##Function to produce the statistical value of differeent variables for Data Schema for larger files like embrium
def summary(filename):
    path = 'T:\\projects\\2019\\EPEC2\\data\\documents_test\\'
    file = path + filename
    read_file = pd.read_csv(file) 
    #print(type(read_file))
    
    df = pd.DataFrame()
    vtype = read_file.dtypes
    #print(type(vtype))
    count = len(read_file)    
    unique = read_file.nunique()
    
    nempty = pd.DataFrame.count(read_file) 
    empty = count - nempty
    describe = read_file.describe().T    
    #measurer = np.vectorize(len)
    #minL = measurer(read_file.values.astype(str)).min(axis=0)
    #maxL = measurer(read_file.values.astype(str)).max(axis=0)

    df['Type'] = vtype    
    df['Count'] = count    
    df['Unique'] = unique
    
    df['Non Empty'] = nempty
    df['Empty'] = empty
    df['Min'] = describe['min']
    df['Max'] = describe['max']
    df['Mean'] = round(describe['mean'], 3)
    df['Std Dev'] = round(describe['std'], 3)
    #df['Min Length'] = minL
    #df['Max Length'] = maxL
    
    name = filename.split('.')[0]
    
    df.to_excel(r'T:\\projects\\2019\\EPEC2\\data\\Output\\Excel\\' + name + '.xlsx')
    
    return df

print(summary('embrium.csv'))

