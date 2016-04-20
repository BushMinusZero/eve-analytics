# Reading the YAML dumps
# Static Data Export (SDE) - https://developers.eveonline.com/resource/static-data-export
# aka Static Data Dump (SDD) - http://wiki.eve-id.net/CCP_Static_Data_Dump

import yaml
import csv


def get_relay(relay_name):
    relays = {'us-west':'tcp://relay-us-west-1.eve-emdr.com:8050',
          'us-central':'tcp://relay-us-central-1.eve-emdr.com:8050',
          'us-east':'tcp://relay-us-east-1.eve-emdr.com:8050',
          'canada-east':'tcp://relay-ca-east-1.eve-emdr.com:8050',
          'german1':'tcp://relay-eu-germany-1.eve-emdr.com:8050',
          'german2':'tcp://relay-eu-germany-2.eve-emdr.com:8050',
          'denmark':'tcp://relay-eu-denmark-1.eve-emdr.com:8050'}
    return relays[relay_name]


def print_yaml(filename):
	stream = open(filename, "r")
	docs = yaml.load_all(stream)
	for doc in docs:
	    for k,v in doc.items():
	        print k, "->", v
	    print "\n"


def import_yaml(filename):
	""" input: filename
		output: writes to cassandra data store
	"""
	stream = open(filename, "r")
	docs = yaml.load_all(stream)
	for doc in docs:
	    for k,v in doc.items():
	    	# WRITE TO CASSANDRA DATA STORE HERE
			pass	        
	    print "\n"	    


def import_schemas(filename):
	""" input: text file with cassandra schema
		output: creates cassandra schema
	"""
	f = open(filename, 'rb')
	raw = f.read()
	print '-------------------------------------------'
	all_commands = raw.split('\n\n')
	for schema in all_commands	:
		print schema
		print '-------------------------------------------'
	return all_commands


def create_ssh_tunnel(ip, port):
	""" Example: ssh -i ~/.ssh/eve-analytics.pem -f ubuntu@54.67.24.31 -L 9042:54.67.24.31:9042 -N
	"""
	pem_file = '~/.ssh/eve-analytics.pem'
	user = 'ubuntu'
	command = "ssh -i {pem_file} -f {user}@{ip} -L {port}:{ip}:{port} -N".format(**locals())
	return None


def read_all_files(data_folder):
    for filename in os.listdir(os.path.join(os.getcwd(),data_folder)):
        with open(os.path.join(data_folder,filename)) as json_file:
            # load json
            print json_file.read()


def get_filenames(data_folder):
    files = []
    for filename in os.listdir(os.path.join(os.getcwd(),data_folder)):
        files.append(filename)
    return files


def read_json(filename, data_folder):
    with open(os.path.join(data_folder,filename)) as json_file:
        return json_file.read()


def generate_date():
    ts = time.time()
    st = datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
    return st
