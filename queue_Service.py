from flask import Flask, request, jsonify
import csv

app = Flask(__name__)

csv_file = 'queue_data.csv'

@app.route('/queue', methods=['POST'])
def add_to_queue():
    records = request.get_json()

    store_records(records)

    return 'Task received and saved into the queue', 200

@app.route('/queue', methods=['GET'])
def get_next_record():
    next_record = get_next_queue_record()
    if next_record:
        return jsonify(next_record)

    return 'No records available in the queue', 404

@app.route('/queue/all', methods=['GET'])
def get_all_records():
    all_records = retrieve_all_records()

    return jsonify(all_records)

@app.route('/queue/<id>', methods=['POST'])
def update_record_status(id):
    status = 'Completed'
    update_record(id, status)

    return 'Status updated successfully'

def store_records(records):
    with open(csv_file, 'a', newline='') as file:
        writer = csv.writer(file)
        for record in records:
            writer.writerow([record['id'], record['value'], 'queue'])

def get_next_queue_record():
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if row[2] == 'queue':
                update_record(row[0], 'processing')
                return {'id': row[0], 'value': row[1], 'status': 'processing'}
    return None

def retrieve_all_records():
    all_records = []
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            record = {
                'id': row[0],
                'value': row[1],
                'status': row[2]
            }
            all_records.append(record)
    return all_records

def update_record(id, status):
    rows = []
    with open(csv_file, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if row[0] == id:
                row[2] = status
            rows.append(row)

    with open(csv_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(rows)

if __name__ == '__main__':
    app.run(debug=True, port=5001)