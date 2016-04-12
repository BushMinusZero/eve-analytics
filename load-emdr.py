import pandas as pd
from datetime import date, datetime, timedelta
import time, math, random
import sql_utilities

class History(object):

    def __init__(self, infile):
        self.infile = infile # 'history_2015-01-13.csv'
        self.hist = pd.read_csv(self.infile)
        
        # computed fields
        self.hist['dt'] = self.hist.date.map(lambda x: datetime.strptime(x.split('T')[0],"%Y-%m-%d"))
        self.hist.sort(['typeID','dt'],inplace=True)
        self.groups = self.find_full_series()
        self.hist['spread'] = self.hist.high - self.hist.low

    def find_full_series(self, days=10, count=10):
        group_dates = self.hist[self.hist.dt>(max(self.hist.dt)-timedelta(days))]
        groups = group_dates[['typeID','regionID','date']].groupby(['typeID','regionID']).date.count().reset_index()
        return groups[groups.date>=count]

    def plot_prices(self, hist, typeID, regionID):
    
        regionName = sql_utilities.lookupRegionName(regionID)
    
        this_hist = hist[(hist.typeID==typeID) & (hist.regionID==regionID)]
        dt = this_hist.dt
        low = this_hist.low
        ave = this_hist.average
        high = this_hist.high

        plt.figure(num=None, figsize=(12, 6), facecolor='w', edgecolor='k')
        plt.plot(dt, low, '-', label='low', linewidth=1.0)
        plt.plot(dt, ave, '-', label='ave', linewidth=1.0)
        plt.plot(dt, high, '-', label='high', linewidth=1.0)
        
        plt.title('TypeID: ' + str(typeID) + '  Region: ' + regionName)
        plt.ylabel('Price')
        plt.xlabel('Date')
        
        plt.show()