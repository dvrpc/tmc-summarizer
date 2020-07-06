import csv
import itertools
import numpy
import time
import os
import pandas as pd

filePath = r"D:\dvrpc_shared\TCExperiment"

rawFiles = []
for root, dirs, files in os.walk(filePath):
    for f in files: 
        if f.endswith(".xls"):
            rawFiles.append(os.path.join(root, f))
			
#create lists of information about each raw count file
countnums = []
IntNames = []
NBstreets = []
SBstreets = []
EBstreets = []
WBstreets = []
dates = []
starttimes = []
endtimes = []

for i in xrange(0, len(rawFiles[0:14])):
    #pull DVRPC count number from filename string based on location
    #numbers will changes based on file path!!! (this could be an issue; needs to be addressed)
    countnums.append(rawFiles[i][29:35])
    xls = pd.ExcelFile(rawFiles[i])
    infodf = pd.read_excel(xls, 'Information')
    IntName = str(infodf.columns[1])
    IntNames.append(IntName)
    NBstreet = str(infodf.loc[0,IntName])
    NBstreets.append(NBstreet)
    SBstreet = str(infodf.loc[1,IntName])
    SBstreets.append(SBstreet)
    EBstreet = str(infodf.loc[2,IntName])
    EBstreets.append(EBstreet)
    WBstreet = str(infodf.loc[3,IntName])
    WBstreets.append(WBstreet)
    
    #date and start/end times
    x = infodf.loc[0,"Unnamed: 4"]
    date = x.strftime("%x")
    dates.append(date)

    starttime = infodf.loc[1, "Unnamed: 4"]
    starttimes.append(starttime)
    endtime = infodf.loc[2, "Unnamed: 4"]
    endtimes.append(endtime)

#spit these lists out in a csv for manual review
#need to review directions and also make sure start and end times line up; if not, edit in excel files (all 3 data tabs)
output_filename = filePath+'\\outputs\\directionreview.csv'
review_colnames = ['countnum', 'intname', 'nbstreet', 'sbstreet', 'ebstreet', 'wbstreet', 'date', 'starttime', 'endtime']

with open(output_filename, mode = 'wb') as review:
    review_writer = csv.writer(review, delimiter = ',')
    review_writer.writerow(review_colnames)
    for i in xrange(0, len(countnums)):
        review_writer.writerow([countnums[i], IntNames[i], NBstreets[i], SBstreets[i], EBstreets[i], WBstreets[i], dates[i], starttimes[i], endtimes[i]])  
		
#create function to rename dataframe columns
def rename_columns(df):
    df.columns = ['Time',
        'SB U Turns',
        'SB Left Turns',
        'SB Thru',
        'SB Right Turns',
        'SB Peds in Crosswalk',
        'WB U Turns',
        'WB Left Turns',
        'WB Thru',
        'WB Right Turns',
        'WB Peds in Crosswalk',
        'NB U Turns',
        'NB Left Turns',
        'NB Thru',
        'NB Right Turns',
        'NB Peds in Crosswalk',
        'EB U Turns',
        'EB Left Turns',
        'EB Thru',
        'EB Right Turns',
        'EB Peds in Crosswalk']
		
#create function to calculate hourly sums for each row
def hoursums(df):
    for i in xrange(0, len(df)):
        if i >= 3:
            SBsum = int(df.iloc[i-3:i+1,   1:5].sum(axis = 1).sum(axis = 0))
            WBsum = int(df.iloc[i-3:i+1,  6:10].sum(axis = 1).sum(axis = 0))
            NBsum = int(df.iloc[i-3:i+1, 11:15].sum(axis = 1).sum(axis = 0))
            EBsum = int(df.iloc[i-3:i+1, 16:20].sum(axis = 1).sum(axis = 0))
            hourtotal = SBsum + WBsum + NBsum + EBsum
            #needs i+2 because it goes by labeled row number, not index row number
            df.set_value(i+2, "Hour Totals", hourtotal)
        #else:
        #    print i, df["Time"].iloc[i]
		
#create empty dictionary and use count numbers as keys
totaldfsums = {}

#for each count file
for i in xrange(0, len(rawFiles[0:14])):
    print countnums[i]
    #read data tabs
    xls = pd.ExcelFile(rawFiles[i])
    totaldf = pd.read_excel(xls, 'Total Vehicles')
    
    #rename columns in dataframes using predefined function
    rename_columns(totaldf)
    
    #remove top 2 rows of dataframe
    totaldf = totaldf.iloc[2:]
    
    #omit rows where Time column = NaN
    totaldf = totaldf[totaldf.Time.notnull()]
    
    #add empty field to hold summary values
    totaldf["Hour Totals"] = 0
    
    #convert object data types to integers to sum (except hour totals - already integer type)
    colnames = totaldf.columns
    
    for j in xrange(0,len(colnames)-1):
        if j > 0:
            totaldf[colnames[j]] = totaldf[colnames[j]].astype(str).astype(int)
            
    #calculate hourly sums using predefined function
    hoursums(totaldf)
    
    #add to dictionary to be summed
    totaldfsums[countnums[i]] = {}
    totaldfsums[countnums[i]] = totaldf['Hour Totals'].tolist()
    
    times = totaldf['Time'].tolist()

#add hour totals across all count files
sumlist = []
for i in xrange(len(totaldfsums.values()[0])-1):
    sum = 0
    for key, value in totaldfsums.iteritems():
        sum += totaldfsums[key][i]
    sumlist.append(sum)
	
#for troubleshooting - when times lenght does not match sumlist length
#remnant of not editing files based on directionreview output early on
#highlights importance of this
times = times[: -1]

#drop into dataframe
allcount_hourlytotals = pd.DataFrame(
    {'Time': times,
     'HourSum': sumlist
    })
	
#find index location for row with AM and PM peak bounds
AMstart = 0
AMend = 0
PMstart = 0
PMend = len(allcount_hourlytotals)+1
for i in xrange(0, len(allcount_hourlytotals)):
    #row, then column
    #6:00AM
    if str(allcount_hourlytotals.iloc[i]['Time']) == '06:00:00':
        AMstart = i
    #10:00AM
    elif str(allcount_hourlytotals.iloc[i]['Time']) == '10:00:00':
        AMend = i
    #4:00PM
    elif str(allcount_hourlytotals.iloc[i]['Time']) == '16:00:00':
        PMstart = i
    #counts typically end before 8:00PM, so just use max
    
print AMstart, AMend, PMstart, PMend

#find max value in column and then start and end times for the calculation of that value
#limited to AM/PM peak hour windows as defined above

#where is the max value in the appropriate time range (AM)
ammaxloc = allcount_hourlytotals['HourSum'][AMstart:AMend+1].idxmax()
#find start and end times for that hour
a = allcount_hourlytotals.loc[ammaxloc+1, 'Time']
b = allcount_hourlytotals.loc[ammaxloc-3, 'Time']
ampeakstart = b.strftime("%H:%M")
ampeakend = a.strftime("%H:%M")
ammax = allcount_hourlytotals['HourSum'][ammaxloc]
print ammax
print "AM Peak", ampeakstart, "to", ampeakend

#where is the max value in the appropriate time range (PM)
pmmaxloc = allcount_hourlytotals['HourSum'][PMstart:len(totaldf)].idxmax()
#find start and end times for that hour
y = allcount_hourlytotals.loc[pmmaxloc+1, 'Time']
z = allcount_hourlytotals.loc[pmmaxloc-3, 'Time']
pmpeakstart = z.strftime("%H:%M")
pmpeakend = y.strftime("%H:%M")
pmmax = allcount_hourlytotals['HourSum'][pmmaxloc]
print pmmax
print "PM Peak", pmpeakstart, "to", pmpeakend

#have user acknowledge AM and PM peak hours before moving on

def outputtmcs_am(df, ampeakstart, ampeakend):
    #find index location for row with AM and PM peak hour bounds found previously
    AMstartrange = 0
    AMendrange   = 0
    for m in xrange(0, len(df)):
        #row, then column
        if str(df.iloc[m][0])   == ampeakstart+':00':
            AMstartrange = m
        elif str(df.iloc[m][0]) == ampeakend  +':00':
            AMendrange = m
    
    #find AM peak counts for each movement
    AM_SBtmc = df.iloc[AMstartrange:AMendrange,   1:5].sum(axis = 0)
    AM_WBtmc = df.iloc[AMstartrange:AMendrange,  6:10].sum(axis = 0)
    AM_NBtmc = df.iloc[AMstartrange:AMendrange, 11:15].sum(axis = 0)
    AM_EBtmc = df.iloc[AMstartrange:AMendrange, 16:20].sum(axis = 0)

    
    AMrow = []
    AMrow.append(countnums[i])
    AMrow.append(IntNames[i])
    AMrow.append('AM')
    for k in xrange(0, len(AM_SBtmc)):
           AMrow.append(AM_SBtmc[k])
    for k in xrange(0, len(AM_WBtmc)):
           AMrow.append(AM_WBtmc[k])
    for k in xrange(0, len(AM_NBtmc)):
           AMrow.append(AM_NBtmc[k])
    for k in xrange(0, len(AM_EBtmc)):
            AMrow.append(AM_EBtmc[k])
    
    return AMrow
	
def outputtmcs_pm(df, pmpeakstart, pmpeakend):
    #find index location for row with AM and PM peak hour bounds found previously
    PMstartrange = 0
    PMendrange   = 0
    for m in xrange(0, len(df)):
        #row, then column
        if str(df.iloc[m][0]) == pmpeakstart+':00':
            PMstartrange = m
        elif str(df.iloc[m][0]) == pmpeakend  +':00':
            PMendrange = m
            
    #find PM peak counts for each movement
    PM_SBtmc = df.iloc[PMstartrange:PMendrange,   1:5].sum(axis = 0)
    PM_WBtmc = df.iloc[PMstartrange:PMendrange,  6:10].sum(axis = 0)
    PM_NBtmc = df.iloc[PMstartrange:PMendrange, 11:15].sum(axis = 0)
    PM_EBtmc = df.iloc[PMstartrange:PMendrange, 16:20].sum(axis = 0)
    
    PMrow = []
    PMrow.append(countnums[i])
    PMrow.append(IntNames[i])
    PMrow.append('PM')
    for k in xrange(0, len(PM_SBtmc)):
           PMrow.append(PM_SBtmc[k])
    for k in xrange(0, len(PM_WBtmc)):
           PMrow.append(PM_WBtmc[k])
    for k in xrange(0, len(PM_NBtmc)):
           PMrow.append(PM_NBtmc[k])
    for k in xrange(0, len(PM_EBtmc)):
           PMrow.append(PM_EBtmc[k])
            
    return PMrow
	
#beginning of this is repeated from for loop above to find peak hours across all counts
#create empty dictionaries to hold dataframes using count numbers as keys
lightdfdict = {}
heavydfdict = {}
totaldfdict = {}

percentheavydict = {}

#for each count file
for i in xrange(0, len(rawFiles[0:14])):
    print countnums[i]
    #read data tabs
    xls = pd.ExcelFile(rawFiles[i])
    lightdf = pd.read_excel(xls, 'Light Vehicles')
    heavydf = pd.read_excel(xls, 'Heavy Vehicles')
    totaldf = pd.read_excel(xls, 'Total Vehicles')
    
    #rename columns in dataframes using predefined function
    rename_columns(lightdf)
    rename_columns(heavydf)
    rename_columns(totaldf)
    
    #remove top 2 rows of dataframe
    lightdf = lightdf.iloc[2:]
    heavydf = heavydf.iloc[2:]
    totaldf = totaldf.iloc[2:]
    
    #omit rows where Time column = NaN
    lightdf = lightdf[lightdf.Time.notnull()]
    heavydf = heavydf[heavydf.Time.notnull()]
    totaldf = totaldf[totaldf.Time.notnull()]
    
    #add empty field to hold summary values
    lightdf["Hour Totals"] = 0
    heavydf["Hour Totals"] = 0
    totaldf["Hour Totals"] = 0
    
    #convert object data types to integers to sum (except hour totals - already integer type)
    colnames = totaldf.columns
    
    for j in xrange(0,len(colnames)-1):
        if j > 0:
            lightdf[colnames[j]] = lightdf[colnames[j]].astype(str).astype(int)
            heavydf[colnames[j]] = heavydf[colnames[j]].astype(str).astype(int)
            totaldf[colnames[j]] = totaldf[colnames[j]].astype(str).astype(int)
            
    #calculate hourly sums using predefined function
    hoursums(lightdf)
    hoursums(heavydf)
    hoursums(totaldf)
    
    #add values to dictionaries to write out later
    lightdfdict[countnums[i]] = {}
    lightdfdict[countnums[i]]['AM'] = outputtmcs_am(lightdf, ampeakstart, ampeakend)
    lightdfdict[countnums[i]]['PM'] = outputtmcs_pm(lightdf, pmpeakstart, pmpeakend)
    
    heavydfdict[countnums[i]] = {}
    heavydfdict[countnums[i]]['AM'] = outputtmcs_am(heavydf, ampeakstart, ampeakend)
    heavydfdict[countnums[i]]['PM'] = outputtmcs_pm(heavydf, pmpeakstart, pmpeakend)
    
    totaldfdict[countnums[i]] = {}
    totaldfdict[countnums[i]]['AM'] = outputtmcs_am(totaldf, ampeakstart, ampeakend)
    totaldfdict[countnums[i]]['PM'] = outputtmcs_pm(totaldf, pmpeakstart, pmpeakend)
	
AMpercentheavy = []
PMpercentheavy = []

for i in xrange(0, len(rawFiles[0:14])):
    #print countnums[i]
   
    for j in xrange(0, len(totaldfdict[countnums[i]]['AM'])):
        if j == 0:
            AMpercentheavy.append(totaldfdict[countnums[i]]['AM'][j])
        elif j == 1:
            AMpercentheavy.append('Percent Heavy Vehicles')
        elif j == 2:
            AMpercentheavy.append(totaldfdict[countnums[i]]['AM'][j])
        else:
            movement_t = float(totaldfdict[countnums[i]]['AM'][j])
            movement_h = float(heavydfdict[countnums[i]]['AM'][j])
            if movement_t == 0.0:
                AMpercentheavy.append(0.0)
            else:
                AMpercentheavy.append((round((movement_h/movement_t)*100,2)))
                
    for k in xrange(0, len(totaldfdict[countnums[i]]['PM'])):
        if k == 0:
            PMpercentheavy.append(totaldfdict[countnums[i]]['PM'][k])
        elif k == 1:
            PMpercentheavy.append('Percent Heavy Vehicles')
        elif k == 2:
            PMpercentheavy.append(totaldfdict[countnums[i]]['PM'][k])
        else:
            movement_t = float(totaldfdict[countnums[i]]['PM'][k])
            movement_h = float(heavydfdict[countnums[i]]['PM'][k])
            if movement_t == 0.0:
                PMpercentheavy.append(0.0)
            else:
                PMpercentheavy.append((round((movement_h/movement_t)*100,2)))
				
percentheavydict = {}
p = 0
q = 19
for i in xrange(0, len(countnums)):
    percentheavydict[countnums[i]] = {}
    percentheavydict[countnums[i]]['AM'] = AMpercentheavy[p:q]
    percentheavydict[countnums[i]]['PM'] = PMpercentheavy[p:q]
    p += 19
    q += 19
	
#spit out peak hour total tmc by movement by intersection
#also include percent heavy vehicles by movement by intersection
output_filename = filePath+'\\outputs\\peakhourtmc_byintersection.csv'
file_colnames = [
        'Count Number',
        'Intersection',
        'Time Period',
        'SB U Turns',
        'SB Left Turns',
        'SB Thru',
        'SB Right Turns',
        'WB U Turns',
        'WB Left Turns',
        'WB Thru',
        'WB Right Turns',
        'NB U Turns',
        'NB Left Turns',
        'NB Thru',
        'NB Right Turns',
        'EB U Turns',
        'EB Left Turns',
        'EB Thru',
        'EB Right Turns']

with open(output_filename, mode = 'wb') as IO:
    writer = csv.writer(IO, delimiter = ',')
    writer.writerow(file_colnames)
    keylist = totaldfdict.keys()
    for i in xrange(0, len(keylist)):
        writer.writerow(totaldfdict[keylist[i]]['AM'])
        writer.writerow(percentheavydict[keylist[i]]['AM'])
        writer.writerow(totaldfdict[keylist[i]]['PM'])
        writer.writerow(percentheavydict[keylist[i]]['PM'])
		
