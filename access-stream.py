import requests
import os
import json
import argparse
import tarfile, zipfile
import tempfile
import time
from pathlib import Path

# Create argument parser
def create_parser():
    parser = argparse.ArgumentParser(description="Script to handle file transfers.")

    # Add arguments
    parser.add_argument("-m", "--mode", choices=["export", "import"],
                        help="Mode of operation: 'export' events from database or 'import' events to database")
    parser.add_argument("-sn", "--stream-name",
                        help="Name of the stream")
    parser.add_argument("-a", "--address", 
                        help="URL for event store database", default="http://localhost:2113")
    parser.add_argument("-f", "--filename",
                        help="Filename for import or export")

    return parser

# Process stream events stored in event store and returns them
def get_stream_events(stream_url, headers):
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

# Export events from event store to the local system 
def export_events(events, file_name, headers):
    file_path = Path(file_name)
    extension = ''.join(file_path.suffixes)

    # Directly export if the target is a .json file
    if extension in ['.json', '']:
        event_file_path = file_path
        export_events_to_file(event_file_path, events, headers)

    # Export to temporary file if the target is compressed
    elif extension in ['.zip', '.tar.xz']:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Construct path to the temporary file within the temp directory
            temp_file_path = Path(f"{Path(temp_dir)}/events.json")
            export_events_to_file(temp_file_path, events, headers)

            # Compress the temporary JSON file
            print(f"Compressing data to {file_name}. This may take a few minutes for larger files.")
            if extension == '.zip':
                with zipfile.ZipFile(file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    zipf.write(temp_file_path, arcname=temp_file_path.name)
            elif extension == '.tar.xz':
                with tarfile.open(file_path, "w:xz") as tar:
                    tar.add(temp_file_path, arcname=temp_file_path.name)
            print(f"Compressed data to {file_name}.")

    else:
        print(f"Unsupported file type: {extension}")


def export_events_to_file(event_file_path, events, headers):
    with open(event_file_path, 'w') as file:
        for entry in reversed(events):  # Reverse to process from oldest to newest
            event_url = entry['links'][0]['uri']
            event_response = requests.get(event_url, headers=headers)
            if event_response.status_code == 200:
                event_data = event_response.json()
                file.write(json.dumps(event_data) + '\n')

# Imports a single event to event store
def import_event(stream_url, event, headers):
    # Construct the payload for the event
    event_payload = [{
        "eventId": event['content']['eventId'],
        "eventType": event['content']['eventType'],
        "data": event['content']['data']
    }]
    
    response = requests.post(stream_url, headers=headers, data=json.dumps(event_payload))

    i = 0
    if response.status_code not in [200, 201]:
        print(f"Failed to upload event. Status code: {response.status_code}, Reason: {response.text}")
        return False
    else:
        return True
        

# Imports all events from a provided file to event store
def import_events(filename, stream_url, headers):
    success_count = 0
    failure_count = 0
    with open(filename, 'r') as file:
        for line in file:
            try:
                event = json.loads(line)
                if import_event(stream_url, event, headers):
                    success_count += 1
                else:
                    failure_count +=1
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")

    if failure_count > 0:
        print(f"{success_count} events successfully uploaded to EventDB, {failure_count} events failed to upload.")
    else:
        print(f"{success_count} events successfully uploaded to EventDB")

# Allows us to import from compressed files
def process_import_file(filename, stream_url, headers):
    file_path = Path(filename)
    extension = ''.join(file_path.suffixes)

    # Directly process if it's a .json file
    if extension in ['.json', '']:
        import_events(filename, stream_url, headers)

    # If it's a compressed file, extract then process the .json files
    elif extension in ['.zip', '.tar.xz']:
        with tempfile.TemporaryDirectory() as temp_dir:
            # Extract the archive to the temporary directory
            print(f"Extracting data from {filename}. This may take a few minutes for large files.")
            if extension == '.zip':
                with zipfile.ZipFile(filename, 'r') as z:
                    z.extractall(path=temp_dir)

            elif extension == ".tar.xz":
                with tarfile.open(filename, "r:xz") as tar:
                    tar.extractall(path=temp_dir)  # Extract all files from the archive
            print(f"Extracted data from {filename}.")

            # Process json file in temporary directory
            for root, dirs, files in os.walk(temp_dir):
                json_files = [f for f in os.listdir(temp_dir) if f.endswith('.json')]

                #TODO: Allow handling of multiple json files in a single call
                if len(json_files) > 1:
                    print("Error: Multiple JSON files in zipped file. System is currently limited to a single file.")
                    exit()
                if len(json_files) == 0:
                    print("Error: No json files in provided file.")
                    exit()

                for json_file in json_files:
                    import_events(os.path.join(root, json_file), stream_url, headers)

    else:
        print(f"Unsupported file type: {extension}")

# EventDB contains "projections", which allow you to interact with streams in a reactive manner
# The follow code allows us to run these projections
def run_projection(base_url, projection_name):
    enable_url = f"{base_url}/projection/{projection_name}/command/enable"
    disable_url = f"{base_url}/projection/{projection_name}/command/disable"
    status_url = f"{base_url}/projection/{projection_name}"

    command_headers = {
        'Content-Length': '0',
    }

    status_headers = {
        'Accept': 'application/json',
    }

    # Start the projection
    response = requests.post(enable_url, headers=command_headers)

    #TODO: Provide additional information about error and how to remedy
    if response.status_code != 200:
        print(f"Failed to start projection. Status code: {response.status_code}")
        exit()

    # Loop until the progress reaches 100.0
    while True:
        response = requests.get(status_url, headers=status_headers)
        if response.status_code == 200:
            data = response.json()
            progress = data.get("progress", 0)
            if progress >= 100.0:
                break
        else:
            print(f"Failed to fetch projection status. Status code: {response.status_code}")
            break

        time.sleep(0.1)  # Wait for 5 seconds before checking again

    # Stop the projection
    response = requests.post(disable_url, headers=command_headers)

    if response.status_code != 200:
        print(f"Failed to stop projection. Status code: {response.status_code}")
    
# Retrieves available streams by accessing the projections function "$streams"
def get_available_streams(base_url, headers):

    # Run the "$stream" projection
    run_projection(base_url, "$streams")

    streams_url = f"{base_url}/streams/$streams"
    response = requests.get(streams_url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        streams = [entry['title'].replace('0@', '', 1) for entry in data['entries']]
        return streams
    else:
        print(f"Failed to list streams. Status code: {response.status_code}")
        return None

# Lists available streams
def list_available_streams(base_url, headers):
    streams = get_available_streams(base_url, headers)

    if streams != None:
        if len(streams) > 0:
            print("Available streams:")
            for index, stream in enumerate(streams, start=1):
                print(f"{index}. {stream}")
            return streams
        else:
            print("Error: No streams are available. Please create one before trying to export.")
    else:
        print("Error: No streams are available. Please create one before trying to export.")
    
    exit()

# Allows the user to select the stream they wish to export from a provided list
def select_stream(base_url, headers):
    streams = list_available_streams(base_url, headers)

    choice = input("Enter the number of the stream to select: ")
    while(True):
        try:
            choice = int(choice) - 1

            if choice >= 0 and choice < len(streams):
                return streams[choice]
            else:
                raise ValueError
        except ValueError:
            choice = input(f"Enter the number of the stream to select. You must provide an integer between 1 and {len(streams)}: ")
            continue

# Allows the user to specify the stream they wish to import to
def specify_stream():
    stream = input("Enter the name of the stream to import to: ")

    while True:
        if stream == "":
            stream = input("Error: No stream specified. Enter the name of the stream to import to: ")
        elif "/" in stream:
            stream = input("Error: '/' is a forbidden character. Enter a valid stream name to import to: ")
        else:
            return stream

# Allows the user to specify the file the wish to import from/export to
def specify_file():
    return input("Please enter the file name: ")

# Validates the provided file
def validate_file(file_name, mode):

    if not file_name:
        file_name = specify_file()
    
    while True:
        file_path = Path(file_name)
        extension = ''.join(file_path.suffixes)

        if extension == "":
            choice = input(f"Warning: Provided file name \"{file_name}\" does not specify an extension. Continue anyway as a .json file? (Y/n): ").strip().lower()
            if choice == "n":
                file_name = specify_file()
                continue
            elif choice not in ("y", ""):
                print("Error: Unknown response. Please enter 'Y' for yes or 'N' for no.")
        elif not file_name.endswith(('.json', '.tar.xz', '.zip')):
            print(f"Error: Provided file type \"{extension}\" is not supported. Valid file types are '.json', '.zip', and '.tar.xz'. Please provide a valid file.")
            file_name = specify_file()
            continue

        if file_name == "":
            print("Error: Provided file name is blank.")
            file_name = specify_file()
            continue

        if mode == "import":
            if not file_path.exists():
                print(f"Error: {file_name} does not exist.")
                file_name = specify_file()
            else:
                return file_path
        elif mode == "export":
            if file_path.exists():
                choice = input(f"Warning: {file_name} already exists. Overwrite? (Y/n): ").strip().lower()
                if choice in ("y", ""):
                    return file_path
                elif choice == "n":
                    file_name = specify_file()
                else:
                    print("Error: Unknown response. Please enter 'Y' for yes or 'N' for no.")
            else:
                return file_path
        else:
            raise ValueError

# Checks whether we will be appending to an existing stream
def validate_stream(stream, base_url, stream_url, headers):
    streams = get_available_streams(base_url, headers)

    while True:
        if stream in streams:
            choice = input("Warning: Stream already exists. Append to it? (y/N): ").strip().lower()
            if choice in ("n", ""):
                stream = specify_stream()
            elif choice == "y":
                return stream
            else:
                print("Error: Unknown response. Please enter 'Y' for yes or 'N' for no.")
        else:
            return stream

# Select mode
def select_mode():
    choice = input("Please select either import (i/import) or export (e/export) mode: ").strip().lower()
    while True:
        if choice in ("i", "import"):
            return "import"
        elif choice in ("e", "export"):
            return "export"
        else:
            choice = input("Error: Unknown response. Please enter either 'I' for import or 'E' for export: ").strip().lower()

if __name__ == "__main__":
    # Parse arguments
    parser = create_parser()
    args = parser.parse_args()

    # EventStoreDB endpoint and stream details
    base_url = args.address
    stream_url = f"{args.address}/streams/"
    stream_name = args.stream_name
    mode = args.mode

    if not mode:
        mode = select_mode()

    if mode == "export":
        headers = {
            "Accept": "application/vnd.eventstore.atom+json",
        }
    elif mode == "import":
        headers = {
            "Content-Type": "application/vnd.eventstore.events+json",
            "Accept": "application/json",
        }

    if not stream_name:
        if mode == "export":
            if not stream_name:
                stream_name = select_stream(base_url, headers)

                if stream_name:
                    print(f"Selected stream: {stream_name}")
                else:
                    print("Error: No stream specified")
                    exit()
        elif mode == "import":
            stream_name = specify_stream()
            stream_name = validate_stream(stream_name, base_url, stream_url, headers)

    stream_url = f"{stream_url}{stream_name}"

    file_path = os.path.expanduser(validate_file(args.filename, mode))

    if mode == "export":
        events = get_stream_events(stream_url, headers)
        if events:
            export_events(events, file_path, headers)
            print(f"Export completed successfully. Total events exported: {len(events)}")
        else:
            print("No events were exported.")
    elif mode == "import":
        process_import_file(file_path, stream_url, headers)
        print("Import process completed.")