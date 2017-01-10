# steps:
# determine which time period you are interested in like [June-1-2016 to June-5-2016]
# make a list of dates
# download all zip files using the list of dates
# unzip them to a file
# loop unzipped text files to make them pandas data frames
# give column names to all those dataframes
# concat them all into one dataframe 
########################################################################################

# determine which time period you are interested in like [June-1-2016 to June-5-2016]
# make a list of dates
import datetime

start = datetime.datetime.strptime("20160601", "%Y%m%d")
end = datetime.datetime.strptime("20160605", "%Y%m%d")
date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]

mydates = []
for date in date_generated:
    mydates.append(date.strftime("%Y%m%d"))

# download all zip files using the list of dates
import requests
import os
base_url = 'https://s3.amazonaws.com/oeda/data/current/events.full.'
ext = '.txt.zip'
default_path = 'C:/Users/user/Documents/phoenixData'
os.chdir(default_path)

for i in mydates:
    r = requests.get(base_url + i + ext)
    with open("phoenix"+i+".zip", "wb") as code:
        code.write(r.content)
        
# loop unzipped text files to make them pandas data frames

path = r"C:/Users/user/Documents/phoenixData"
paths = []
for i in range(len(mydates)):
    paths.append(path + '/' + 'phoenix' + mydates[i] + '/')

#Now we have all the paths needed. We need to create a dataframe for each of these paths.

allFrames = []
for i in range(len(paths)):
    try:
        df = pd.read_table((paths[i]+'events.full.'+mydates[i]+'.txt'))
        allFrames.append(df)
    except:
        print("NoDataThere")

# give column names to all those dataframes
    
for i in range(len(allFrames)):
    allFrames[i].columns = ['EventID', 
                            'Date',
                            'Year',
                            'Month',
                            'Day',
                            'SourceActorFull',
                            'SourceActorEntity',
                            'SourceActorRole',
                            'SourceActorAttribute',
                            'TargetActorFull',
                            'TargetActorEntity',
                            'TargetActorRole',
                            'TargetActorAttribute',
                            'EventCode',
                            'EventRootCode',
                            'PentaClass',
                            'GoldsteinScore',
                            'Issues',
                            'Lat',
                            'Lon',
                            'LocationName',
                            'StateName',
                            'CountryCode',
                            'SentenceID',
                            'URLs',
                            'NewsSources']


# concat them all into one dataframe 
dfConcat = pd.concat(allFrames)

# get some info about the concat.dataframe
dfConcat.info()
