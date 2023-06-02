import json
import csv
import boto3
import time

nodes_file = 'nodes.csv'
stream_name = ''
time_delay = 0.2


client = boto3.client('kinesis')


json_record_format = '''
	{
		'~id': '{f1}',
		'~label': '{f2}',
		'first_name': '{f3}',
		'last_name': '{f4}',
		'date': '{f5}'
	'''


def get_json_record(node_id, label, first_name, last_name, date):
	json_string = json_record_format.format(f1=node_id, f2=label, f3=first_name, f4=last_name, f5=date)
	return json.dumps(json_string)


def put_record(json_record):
	response = client.put_record(
		StreamName = stream_name,
		Data = json_record,
		PartitionKey = json_record['~id']
	)

	if response['ResponseMetadata']['HTTPStatusCode'] != 200:
		print('Error putting node #{} to stream {}'.format(json_record['~id'], stream_name))
	else:
		print('Successfully put node#{} to stream {}'.format(json_record['~id'], stream_name))


def stream_nodes(num_records):
	with open(nodes_file, 'r') as csv_file:
		csv_reader = csv.reader(csv_file)
		time.sleep(time_delay)
		for row in csv_reader:
			node_id, label, first_name, last_name, date = tuple(row)
			json_record = get_json_record(node_id, label, first_name, last_name, date)


def main():
	stream_nodes()


if __name__ == '__main__':
	main()
