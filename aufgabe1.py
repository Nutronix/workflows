import requests
import json

# URL der API für die Rohdaten
dataset_url = 'http://localhost:8080/v1/dataset'
result_url = 'http://localhost:8080/v1/result'

# Funktion, um die Daten von der API zu holen
def get_data_from_api():
    response = requests.get(dataset_url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to retrieve data: {response.status_code}")

# Funktion zur Berechnung der Gesamtnutzungszeit
def calculate_total_runtime(data):
    customer_runtime = {}
    
    for event in data["events"]:
        customer_id = event["customerId"]
        workload_id = event["workloadId"]
        timestamp = event["timestamp"]
        event_type = event["eventType"]
        
        # Initialisiere den Kunden, falls er noch nicht existiert
        if customer_id not in customer_runtime:
            customer_runtime[customer_id] = {}

        # Wenn es ein Start-Event ist, speichere den Startzeitpunkt
        if event_type == "start":
            customer_runtime[customer_id][workload_id] = timestamp
        
        # Wenn es ein Stopp-Event ist, berechne die Laufzeit und addiere sie
        elif event_type == "stop":
            start_time = customer_runtime[customer_id].get(workload_id, 0)
            runtime = timestamp - start_time
            customer_runtime[customer_id][workload_id] = runtime

    # Aggregiere die Gesamtlaufzeit pro Kunde
    total_runtime = {}
    for customer_id, workloads in customer_runtime.items():
        total_runtime[customer_id] = sum(workloads.values())
    
    return total_runtime

# Funktion, um das Ergebnis an die API zu senden
def send_result_to_api(result):
    payload = {"result": [{"customerId": k, "consumption": v} for k, v in result.items()]}
    response = requests.post(result_url, json=payload)
    if response.status_code == 200:
        print("Result successfully sent!")
    else:
        raise Exception(f"Failed to send result: {response.status_code}")

def main():
    # Daten von der API abrufen
    data = get_data_from_api()
    
    # Gesamtlaufzeit berechnen
    total_runtime = calculate_total_runtime(data)
    
    # Ergebnis an die API senden
    send_result_to_api(total_runtime)

if __name__ == '__main__':
    main()
