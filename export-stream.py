import requests
import json

# EventStoreDB endpoint and stream details
base_url = 'http://localhost:2113/streams/'
stream_name = "2024-02-13-1416502e3aa672-ae57-47e9-b2d4-409b5aab0058"
headers = {
    "Accept": "application/vnd.eventstore.atom+json",
}

def get_stream_events(stream_url):
    events = []
    while stream_url:
        response = requests.get(stream_url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to fetch stream page. Status code: {response.status_code}")
            break

        data = response.json()
        events.extend(data['entries'])
        
        # Find the 'next' link to continue pagination
        prev_link = next((link for link in data['links'] if link['relation'] == 'next'), None)
        if prev_link:
            stream_url = prev_link['uri']
        else:
            break  # No more pages to fetch
    
    return events

def export_events(events, filename='exported_stream.json'):
    with open(filename, 'w') as file:
        for entry in reversed(events):  # Reverse to process from oldest to newest
            event_url = entry['links'][0]['uri']
            event_response = requests.get(event_url, headers=headers)
            if event_response.status_code == 200:
                event_data = event_response.json()
                file.write(json.dumps(event_data) + '\n')

if __name__ == "__main__":
    stream_url = f"{base_url}{stream_name}"
    events = get_stream_events(stream_url)
    if events:
        export_events(events)
        print(f"Export completed successfully. Total events exported: {len(events)}")
    else:
        print("No events were exported.")

