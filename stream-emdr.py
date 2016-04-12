#!/usr/bin/env python

import zlib
import zmq
import simplejson
import json
import time

def setup_subscriber():

    relays = {'us-west':'tcp://relay-us-west-1.eve-emdr.com:8050',
              'us-central':'tcp://relay-us-central-1.eve-emdr.com:8050',
              'us-east':'tcp://relay-us-east-1.eve-emdr.com:8050',
              'canada-east':'tcp://relay-ca-east-1.eve-emdr.com:8050',
              'german1':'tcp://relay-eu-germany-1.eve-emdr.com:8050',
              'german2':'tcp://relay-eu-germany-2.eve-emdr.com:8050',
              'denmark':'tcp://relay-eu-denmark-1.eve-emdr.com:8050'}

    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)

    # Connect to the first publicly available relay.
    subscriber.connect(relays['us-west'])
    
    # Disable filtering.
    subscriber.setsockopt(zmq.SUBSCRIBE, "")
    return subscriber


def stream_data_to_file(subscriber, data_dir, data_prefix):

    while True:
        market_json = zlib.decompress(subscriber.recv())
        market_data = simplejson.loads(market_json)
        f = data_dir + data_prefix + str(time.time()) + '.txt'
        with open(f, 'w') as outfile:
            json.dump(market_data, outfile)


def main():
    """Example Python EMDR client."""

    subscriber = setup_subscriber()

    data_dir = 'data/stream/'
    data_prefix = 'emdr'

    print "[*] Beginning data stream."
    stream_data_to_file(subscriber, data_dir, data_prefix)


if __name__ == '__main__':
    main()