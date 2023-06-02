from faker import Faker
import datetime as dt
import pandas as pd
import random
import boto3
import json
import uuid
import sys
import csv


fake = Faker()

# files for dummy data
nodes_file = 'nodes.csv'
edges_file = 'edges.csv'


# headers for the dummy data
node_headers = ['~id', '~label', 'first_name', 'last_name', 'date']
edge_headers = ['~id', '~from', '~to', '~label']
relation_types = ['boss_of', 'coworker_of', 'friend_of', 'enemy_of', 'relative_of', 'teacher_of', 'disciple_of']
labels = ['CEO', 'company_owner', 'driver', 'software_developer', 'janitor', 'pilot', 'teacher', 'surgeon', 'nurse', 'real_estate_agent']




def generate_random_edges_file(num_rows):
	print('Generating edges')
	data = []
	node_ids = list(pd.read_csv(nodes_file)['~id'])

	for i in range(num_rows):
		edge_id = str(uuid.uuid4())
		from_id = random.choice(node_ids)
		to_id = random.choice(node_ids)
		while to_id == from_id:
			to_id = random.choice(node_ids)
		relation_type = random.choice(relation_types)
		row = [edge_id, from_id, to_id, relation_type]
		print(row)
		data.append(row)


	with open(edges_file, 'w+', newline='') as csv_file:
		csv_writer = csv.writer(csv_file)
		csv_writer.writerow(edge_headers)
		csv_writer.writerows(data)




def generate_random_nodes_file(num_rows):
	print('Generating edges')
	data = []
	date_range_low = dt.datetime.strptime(f'1/1/1990', '%m/%d/%Y')
	date_range_high = dt.datetime.strptime(f'6/2/2023', '%m/%d/%Y')

	for i in range(num_rows):
			node_id = str(uuid.uuid4())
			label = random.choice(labels)
			first_name, last_name = fake.name().split()[0], fake.name().split()[1]
			date = fake.date_between(date_range_low, date_range_high)
			string_date = date.strftime("%m/%d/%Y")
			row = [node_id, label, first_name, last_name, string_date]
			print(row)
			data.append(row)
	
	with open(nodes_file, 'w+', newline='') as csv_file:
		csv_writer = csv.writer(csv_file)
		csv_writer.writerow(node_headers)
		csv_writer.writerows(data)


def main():
	generate_random_nodes_file(int(sys.argv[1]))
	generate_random_edges_file(int(sys.argv[2]))


if __name__ == '__main__':
	main()


