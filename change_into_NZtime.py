import glob
import pandas as pd
import csv
import numpy as np
from datetime import time
from datetime import datetime
from datetime import timedelta

##Path mentioned
path = 'T:\\projects\\2019\\EPEC2\\data\CSV-output\\redback\\redbackSummer.csv'

##Read csv file
redbackSummer = pd.read_csv(path)
#print(events.info())

##Assigning the date variable
old_date = redbackSummer['_source.date']

#print("helloolddate"+ old_date.head(5))

#Function to change the UTC date NZDT date
def change_into_NZdate(year, start_date, end_date):
    newdate = []
    for d in old_date:
        if d[:4] == year:
            if d >= start_date:
                if d < end_date:
                    #if the date lies inside the range, +12 hours
                    tempd = datetime.strptime(d,"%Y-%m-%dT%H:%M:%S.%fZ")
                    dt = tempd + timedelta(hours = 12)
                    newd = dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                    newdate.append(newd)
                else:
                    #if the date is greater than the start date and end date, +13 hours
                    tempd = datetime.strptime(d,"%Y-%m-%dT%H:%M:%S.%fZ")
                    dt = tempd + timedelta(hours = 13)
                    newd = dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                    newdate.append(newd)                            
            else:
            #if date is outside the range, +13 hours
                tempd = datetime.strptime(d,"%Y-%m-%dT%H:%M:%S.%fZ")
                dt = tempd + timedelta(hours = 13)
                newd = dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                newdate.append(newd)
                    
    return newdate

##Converted date is saved for each year in each variable as the day light saving dates are different in each year
newdate_2016 = change_into_NZdate('2016', '2016-04-02T14:00:00.000Z','2016-09-24T14:00:00:000Z')
newdate_2017 = change_into_NZdate('2017', '2017-04-01T14:00:00.000Z','2017-09-23T14:00:00:000Z')
newdate_2018 = change_into_NZdate('2018', '2018-03-31T014:00:00.000Z','2018-09-29T14:00:00:000Z')
newdate_2019 = change_into_NZdate('2019', '2019-04-06T14:00:00.000Z','2019-09-28T14:00:00:000Z')

#print(len(newdate_2016))

#print(len(newdate_2017))
#print(len(newdate_2018))
#print(len(newdate_2019))

##All the converted dates are stored together in a dataframe
newdate_frame = []
newdate_frame = newdate_2016 + newdate_2017 + newdate_2018 + newdate_2019
print(len(old_date))
print(len(newdate_frame))

redbackSummer['new_date'] = newdate_frame

print(redbackSummer.info())

##The new drataframe is saved as csv files along with all the variables
redbackSummer.to_csv(r'/Users/jing/Documents/Master of Data science/601/project/data/new_redbackSummer.csv')