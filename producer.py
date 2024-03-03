from flask import Flask, request, jsonify
from circuit_breaker import circuit_breaker
import sys
sys.tracebacklimit = 0
import requests
import uuid

app = Flask(__name__)
message_queue = []

@app.route('/endpoint', methods=['POST'])
@circuit_breaker()
def receive_values():
    values = request.json

    processed_values = process_values(values)
    message_queue.extend(processed_values)
    
    try:
        response = requests.post('http://localhost:5001/queue', json=processed_values)
        if response.status_code == 200:
            print("POST request to queue_Service successful")
        else:
            print(f"POST request to queue_Service failed")
            raise Exception("Server Issue")
    except Exception:
        raise Exception("Server Issue")
    return jsonify(message_queue)

def process_values(values):
    processed_values = []
    for value in values:
        uid = str(uuid.uuid4())

        processed_value = {'id': uid, 'value': value}

        processed_values.append(processed_value)
    
    return processed_values

if __name__ == '__main__':
    app.run(debug=True)