#!/usr/bin/env python

import pandas as pd
import numpy as np
import json
import os
import time
from datetime import datetime
import utils


def parse_raw_emdr_data(data_folder):

    filenames = utils.get_filenames(data_folder)

    total_orders = []
    total_history = []
    for f in filenames:
        try:
            j = json.loads(utils.read_json(f, data_folder))
            rowsets = j['rowsets']
            resultType = j['resultType']
                    
            if resultType=='orders':
                for rowset in rowsets:
                    for row in rowset['rows']:
                        total_orders.append(row)

            elif resultType=='history':
                for rowset in rowsets:
                    typeID = rowset['typeID']
                    regionID = rowset['regionID']
                    for row in rowset['rows']:
                        row.append(typeID)
                        row.append(regionID)
                        total_history.append(row)

            else:
                print '[x] Result type is not orders or history.'

        except Exception as e:
            print 'Filename: ' + f
            print e

    return total_orders, total_history


def emdr_to_csv(input_dir, output_dir):

    total_orders, total_history = parse_raw_emdr_data(input_dir)

    if len(total_orders)>0:
        orders_cols = ["price", "volRemaining", "range", "orderID", "volEntered", "minVolume", "bid", "issueDate", "duration", "stationID", "solarSystemID"]
        orders = pd.DataFrame(total_orders, columns=orders_cols)
        orders.to_csv(os.path.join(output_dir, 'orders_' + utils.generate_date() + '.csv'), index=False)
    else:
        orders=[]

    if len(total_history)>0:
        history_cols = ["date", "orders", "quantity", "low", "high", "average","typeID","regionID"]
        history = pd.DataFrame(total_history, columns=history_cols)
        history.to_csv(os.path.join(output_dir, 'history_' + utils.generate_date() + '.csv'), index=False)
    else:
        history=[]

    return orders, history


def main():

    input_dir = 'data/stream/'
    output_dir = 'data/output/'
    emdr_to_csv(input_dir, output_dir)


if __name__ == '__main__':
    main()