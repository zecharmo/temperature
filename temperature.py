import datetime
import requests
import sqlite3 as lite
import pandas as pd
from pandas.io.json import json_normalize
import collections

api_key = "key"
link = 'https://api.forecast.io/forecast/' + api_key

cities = {'Atlanta' : '33.762909,-84.422675',
'Austin' : '30.303936,-97.754355',
'Boston' : '42.331960,-71.020173',
'Chicago' : '41.837551,-87.681844',
'Cleveland' : '41.478462,-81.679435',
'Denver' : '39.761850,-104.881105',
'Las Vegas' : '36.229214,-115.26008',
'Los Angeles' : '34.019394,-118.410825',
'Miami' : '25.775163,-80.208615',
'Minneapolis' : '44.963324,-93.268320',
'Nashville' : '36.171800,-86.785002',
'New Orleans' : '30.053420,-89.934502',
'New York' : '40.663619,-73.938589',
'Philadelphia' : '40.009376,-75.133346',
'Phoenix' : '33.572154,-112.090132',
'Salt Lake City' : '40.778996,-111.932630',
'San Francisco' : '37.727239,-123.032229',
'Seattle' : '47.620499,-122.350876',
'Washington' : '38.904103,-77.017229'}

# define time frame for analysis, start - 30 days ago, end - today
end_date = datetime.datetime.now()
start_date = datetime.datetime.now() - datetime.timedelta(seconds=2592000)
# will need to reset the count before each loop
original_start_date = start_date

# create database
con = lite.connect('weather.db')
cur = con.cursor()

cities.keys()
with con:
    cur.execute("CREATE TABLE daily_temp ('day_of_reading' TEXT, 'Atlanta' REAL, 'Austin' REAL, 'Boston' REAL,  
		'Chicago' REAL, 'Cleveland' REAL, 'Denver' REAL, 'Las Vegas' REAL, 'Los Angeles' REAL, 'Miami' REAL, 
		'Minneapolis' REAL, 'Nashville' REAL, 'New Orleans' REAL, 'New York' REAL, 'Philadelphia' REAL,  
		'Phoenix' REAL, 'Salt Lake City' REAL, 'San Francisco' REAL, 'Seattle' REAL, 'Washington' REAL);")

with con:
    while start_date < end_date:
        cur.execute("INSERT INTO daily_temp(day_of_reading) VALUES (?)", (start_date.strftime('%Y-%m-%dT%H:%M:%S'),))
        start_date += datetime.timedelta(seconds=86400)
start_date = original_start_date

for k,v in cities.iteritems():
    start_date = original_start_date
    while start_date < end_date:
        #query for the value
        r = requests.get(link + '/' v + ',' +  start_date.strftime('%Y-%m-%dT%H:%M:%S'))
		
		SQL_line = "UPDATE daily_temp SET '{0}' = {1} WHERE day_of_reading = '{2}'".format(k, str(r.json()['daily']['data'][0]['temperatureMax']), start_date.strftime('%Y-%m-%dT%H:%M:%S'))

        with con:
            #insert the temperature max to the database
            cur.execute(SQL_line)

        #increment start_date to the next day for next operation of loop
        start_date += datetime.timedelta(seconds=86400)


con.close()

# reconnect to the database to begin analysis
con = lite.connect('weather.db')
cur = con.cursor()

# calculate range 
temp_max = df[['Atlanta', 'Austin', 'Boston', 'Chicago', 'Cleveland', 'Denver', 'Las Vegas', 'Los Angeles', 'Miami', 'Minneapolis', 'Nashville', 'New Orleans', 'New York', 'Philadelphia', 'Phoenix', 'Salt Lake City', 'San Francisco', 'Seattle', 'Washington']].max()
temp_min = df[['Atlanta', 'Austin', 'Boston', 'Chicago', 'Cleveland', 'Denver', 'Las Vegas', 'Los Angeles', 'Miami', 'Minneapolis', 'Nashville', 'New Orleans', 'New York', 'Philadelphia', 'Phoenix', 'Salt Lake City', 'San Francisco', 'Seattle', 'Washington']].min()
temp_var = temp_max - temp_min
# calculate which city had the greatest variation in temperature
print temp_var.max()
# output: 29.61, Chicago

# SQLite query to sort data by execution time
df = pd.read_sql_query("SELECT * FROM daily_temp ORDER BY day_of_reading",con,index_col='day_of_reading')

temperature_change = collections.defaultdict(int)
for col in df.columns:
    temps = df[col].tolist()
    temp_change = 0
    for k,v in enumerate(temps):
        if k < len(temps) - 1:
            temp_change += abs(temps[k] - temps[k+1])

# create a list of the dictionary's keys and values
# goal is to get the highest temperature for each 
def keywithmaxval(d):
    v = list(d.values())
    k = list(d.keys())

    # return the key with the max value
    return k[v.index(max(v))]

# assign the max key to the city
max_temp = keywithmaxval(temperature_change)

# query SQLite for reference information
cur.execute("SELECT * FROM daily_temp WHERE id = ?", (max_temp,))
data = cur.fetchone()
print "The highest temperature range in the last month was in %s with a range of %s degrees " % data
# output: The highest temperature in the last month was in Chicago with a range of 29.61 degrees 

# plot bar graph to show temperature values
plt.bar(temperature_change.keys(), temperature_change.values())
plt.show()