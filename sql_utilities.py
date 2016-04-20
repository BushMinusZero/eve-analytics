import sqlite3
import pandas as pd

""" SQLite Utility Functions for Ingesting Eve Lookup Values
"""
class RheaTable(object):

    def __init__(self, table_name, var_names, var_types):
        self.table_name = table_name
        self.var_names = var_names
        self.var_types = var_types

    def print_schema(self):
        print '[*] ' + self.table_name
        for i,var in enumerate(self.var_names):
            print var + ' (' + var_types[i] + ')'
        print ''

    def connect(self, data_loc, filename):
        conn = sqlite3.connect(data_loc + filename)
        self.c = conn.cursor()

    def run_query(self,query_sql):
        self.c.execute(query_sql)
        return self.c.fetchall()

class RegionTable(RheaTable):

    def __init__(self):
        self.table_name = 'mapRegions'
        self.mapRegionsSchema = {'vars': 
                    [('regionID','integer'),
                     ('regionName', 'varchar(100)'),
                     ('x', 'real'),
                     ('y', 'real'),
                     ('z', 'real'),
                     ('xMin', 'real'),
                     ('xMax', 'real'),
                     ('yMin', 'real'),
                     ('yMax', 'real'),
                     ('zMin', 'real'),
                     ('zMax', 'real'),
                     ('factionID', 'integer'),
                     ('radius', 'real')],
                    'primary':'regionID'}
        self.var_names = [row[0] for row in self.mapRegionsSchema]
        self.var_types = [row[1] for row in self.mapRegionsSchema]

    def getRegionName(self,regionID):
        nameQuery = """select regionName from {self.table_name} where regionID={regionID}"""
        return self.run_query(nameQuery.format(**locals()))[0][0]

class ItemTable(RheaTable):

    def __init__(self):
        self.table_name = 'mapDenormalize'
        self.mapDenormalizeSchema = {'vars':
                                         [('itemID', 'integer '),
                                          ('typeID', 'integer'),
                                          ('groupID', 'integer'),
                                          ('solarSystemID', 'integer'),
                                          ('constellationID', 'integer'),
                                          ('regionID', 'integer'),
                                          ('orbitID', 'integer'),
                                          ('x', 'real'),
                                          ('y', 'real'),
                                          ('z', 'real'),
                                          ('radius', 'real'),
                                          ('itemName', 'varchar(100)'),
                                          ('[security]', 'real'),
                                          ('celestialIndex', 'integer'),
                                          ('orbitIndex', 'integer')],
                                     'primary': 'itemID'}
        self.var_names = [row[0] for row in self.mapDenormalizeSchema]
        self.var_types = [row[1] for row in self.mapDenormalizeSchema]

    def getItemName(self,itemID):
        """ input: itemID
            output: itemName
        """
        nameQuery = """select itemName from {self.table_name} where itemID={itemID}"""
        return self.run_query(nameQuery.format(**locals()))[0][0]

def lookupRegionName(regionID):
    # location of database
    data_location = 'emdrData/Rhea_1/'
    filename = 'universeDataDx.db'

    mapRegions = RegionTable()
    mapRegions.connect(data_location, filename)
    regionName = mapRegions.getRegionName(regionID)
    return regionName

def lookupItemName(itemID):
    # location of database
    data_location = 'emdrData/Rhea_1/'
    filename = 'universeDataDx.db'

    itemTable = ItemTable()
    itemTable.connect(data_location, filename)
    itemName = itemTable.getItemName(itemID)
    return itemName


""" UAT TEST ITEMID==TYPEID ASSUMPTION (ASSUMPTION IS FALSE -- USE THE YAML INSTEAD

data_location = 'emdrData/Rhea_1/'
filename = 'universeDataDx.db'
conn = sqlite3.connect(data_location + filename)
c = conn.cursor()

c.execute("select itemName, typeID, 1 as cnt from mapDenormalize")
r0 = c.fetchall()
df = pd.DataFrame(r0,columns=['itemName','typeID','cnt'])
cnts =  df.groupby(['itemName','typeID']).cnt.count().reset_index()

data_location = 'emdrData/Rhea_1/'
filename = 'universeDataDx.db'
conn = sqlite3.connect(data_location + filename)
c = conn.cursor()

typeID = 20
#c.execute("select * from mapDenormalize where typeID={typeID}".format(**locals()))
c.execute("select typeID, itemName from mapDenormalize".format(**locals()))
r0 = c.fetchall()
df = pd.DataFrame(r0, columns=['typeID','itemName'])
print df
"""
