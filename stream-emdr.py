""" 
    Example Python EMDR client.
"""
import os
import zlib
import zmq
import json
import time
import utils

class EMDR(object):

    def __init__(self, relay_name='us-west'):
        self.relay_name = relay_name
        self.relay = get_relay(relay_name)
        self.subscriber = self.connect()


    def connect(self):
        context = zmq.Context()
        subscriber = context.socket(zmq.SUB)

        subscriber.connect(self.relay)
        subscriber.setsockopt(zmq.SUBSCRIBE, "")
        return subscriber


    def stream_to_file(self, data_dir, data_prefix):

        self.data_dir = data_dir
        self.data_prefix = data_prefix

        while True:
            market_json = zlib.decompress(self.subscriber.recv())
            market_data = json.loads(market_json)
            filename = os.path.join(data_dir, data_prefix + str(time.time())) + '.txt'
            with open(filename, 'w') as outfile:
                json.dump(market_data, outfile)


def main():
    """Example Python EMDR client."""

    data_dir = 'data/stream/'
    data_prefix = 'emdr'

    emdr = EMDR()

    print "[*] Beginning data stream."
    emdr.stream_to_file(data_dir, data_prefix)


if __name__ == '__main__':
    main()