from flask import Flask, request
import requests
import csv
from datetime import datetime

app = Flask(__name__)

processed_file = 'Processed_Files.csv'

@app.route('/starProcess', methods=['POST'])
def start_process():
    initialize_processed_file()
    
    while True:
        response = requests.get('http://localhost:5001/queue')
        
        if response.status_code == 200:
            next_record = response.json()

            response = requests.post(f"http://localhost:5001/queue/{next_record['id']}")
            
            if response.status_code == 200:
                append_record_to_processed_file(next_record)
                continue
            else:
                return 'There are no more records to process'
        
        break
    
    return 'No records available in the queue'

@app.route('/AllRecords', methods=['GET'])
def get_all_records():
    all_records = retrieve_all_records()
    
    return all_records

@app.route('/ProcessOneRecord', methods=['POST'])
def process_one_record():
    initialize_processed_file()
    data = request.get_json()
    process_record(data)
    
    append_record_to_processed_file(data)

    response = requests.post(f"http://localhost:5001/queue/{data['id']}")
    
    if response.status_code == 200:
        return 'Record processed successfully'
    else:
        return 'Error updating the record status'

def initialize_processed_file():
    try:
        with open(processed_file, 'r') as file:
            pass
    except IOError:
        with open(processed_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['ID', 'Value', 'Process Date'])

def process_record(record):
    pass

def append_record_to_processed_file(record):
    with open(processed_file, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([record['id'], record['value'], datetime.now()])

def retrieve_all_records():
    all_records = []
    with open(processed_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)  
        for row in reader:
            record = {
                'id': row[0],
                'value': row[1],
                'process_date': row[2]
            }
            all_records.append(record)
    return all_records

if __name__ == '__main__':
    app.run(debug=True, port=5002)