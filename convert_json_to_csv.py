import os
import json
import pandas as pd
from pandas.io.json import json_normalize


#Function to combine all the JSON files to one single CSV file suitable for small size folders
def get_dataframe(foldname):
    directory = 'T:\\projects\\2019\\EPEC2\\data\\Json-output\\'
    path = directory + foldname
    #list all the sub-folders
    file_list = os.listdir(path)    
    df = pd.DataFrame()
    csv_name = '\\' + foldname + '.csv'
    for file in file_list:
        filepath = path+"\\"+file
        #list all the files inside the sub-folder
        filename = os.listdir(filepath)
       
        for single_file in filename:
            with open(filepath + "\\" + single_file) as f:
            #each json file is taken
                line = f.readline()
                data = json.loads(line)
                values = data['hits']['hits']
                normal_data = pd.DataFrame.from_dict(json_normalize(values), orient='columns')
            #Concatinate with the previous json file as a dataframe
            df = pd.concat([df, normal_data], sort=True)
            
            #print(csv_name)
            
    #save the dataframe as a single csv file        
    export_csv = df.to_csv(r'T:\projects\2019\EPEC2\data\CSV-output' + csv_name, index = None, header = True)
    return foldname

folder_name = get_dataframe("s3files")
folder_name[:1]


#Function to combine all the JSON files to multiple CSV files under the month name because of the large size of the folder
def get_big_dataframe(main_folder):
    directory = "T:\\projects\\2019\\EPEC2\\data\\Json-output\\"
    path = directory + main_folder
    #list all the sub-folders
    file_list = os.listdir(path)

    for file in file_list:
        file_path = path + "\\" + file
        #list all the files inside the sub-folder
        file_name = os.listdir(file_path)
        df = pd.DataFrame()
        print(file)
        for single_file in file_name:
            # print(single_file)
            with open(file_path + "\\" + single_file) as f:
                #each json file is taken
                line = f.readline()
                data = json.loads(line)
                values = data['hits']['hits']
                normal_data = pd.DataFrame.from_dict(json_normalize(values), orient='columns')
            #Concatinate with the previous json file as a dataframe
            df = pd.concat([df, normal_data], sort=True)
            
        #New folder is created if it doesn't exist under the main folder name
        if not os.path.exists('T:\\projects\\2019\\EPEC2\\data\CSV-output\\test' + main_folder):
            os.makedirs('T:\\projects\\2019\\EPEC2\\data\CSV-output\\test' + main_folder)
        csv_name = file + ".csv"
        #save the dataframe as a single csv file for each available months 
        foldername_csv = df.to_csv(r'T:\\projects\\2019\\EPEC2\\data\CSV-output\\test' + main_folder + '\\' + csv_name,
                                   index=None, header=True)

    return df

folder_name = get_big_dataframe("redback")
folder_name[:1]



