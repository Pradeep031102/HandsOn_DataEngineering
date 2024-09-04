##-1 
import json

file_path = r"C:\Users\prade\Documents\python\data.json"

with open(file_path, 'r') as file:
    data = json.load(file)
    print(data)


#2

import json

profile = {
    "name": "Jack",
    "age": 28,
    "city": "Chennai",
    "hobbies": ["Photography", "Traveling", "Cooking"]
}

file_path = r'C:\Users\prade\Documents\python\profile.json'

with open(file_path, 'w') as file:
    json.dump(profile, file, indent=4)


#3

import csv
import json

csv_file_path = r'C:\Users\prade\Documents\python\students.csv'
json_file_path = r'C:\Users\prade\Documents\python\students.json'

with open(csv_file_path, 'r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    data = list(csv_reader)

with open(json_file_path, 'w') as json_file:
    json.dump(data, json_file, indent=4)


#4
import csv
import json

json_file_path = r'C:\Users\prade\Documents\python\data.json'
csv_file_path = r'C:\Users\prade\Documents\python\data.csv'

with open(json_file_path, 'r') as json_file:
    data = json.load(json_file)

with open(csv_file_path, 'w', newline='') as csv_file:
    csv_writer = csv.writer(csv_file)
    csv_writer.writerow(data.keys())
    csv_writer.writerow(data.values())


#5

import json

file_path = r'C:\Users\prade\Documents\python\books.json'

with open(file_path, 'r') as file:
    data = json.load(file)

for book in data['books']:
    print(book['title'])
