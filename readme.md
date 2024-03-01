# Event Store Transfer Script

This script is designed to facilitate the import of events to and the export of events from an Event Store database. The script is versatile, allowing for operations on specific streams and handling both plain JSON and compressed files.

## Features

- **Export Events**: Extract events from a specified stream and save them locally as JSON or within a compressed file (.zip or .tar.xz).
- **Import Events**: Load events from a local JSON or compressed file and import them into a specified stream.
- **Stream Management**: In interactive mode, lists available streams from the Event Store database and select streams for import/export operations. In all modes, confirms with the user prior to appending data to an existing stream.
- **Flexible File Handling**: Supports exporting to and importing from json, zip, and tar.xz files.

## Requirements

- Python 3.6 or higher
- `requests` library
- `tqdm` library

Before running the script, ensure you have the necessary libraries installed. You can install them using pip:

```bash
pip install requests
pip install tqdm
```

## Usage

### Command Line Arguments

The script supports several command line arguments for specifying its operation mode, target stream, Event Store database address, and the filename for import/export:

- `-m`, `--mode`: Mode of operation (`export` or `import`).
- `-sn`, `--stream-name`: Name of the stream for import/export.
- `-a`, `--address`: URL of the Event Store database (default: `http://localhost:2113`).
- `-f`, `--filename`: Filename for the import/export operation.

### Running the Script

To use the script, you can specify all required information through command line arguments. Alternatively, you can specify some or none and operate the script in interactive mode. When exporting with interactive mode, a list of streams will be provided to select from.

### Examples

```bash
python access-stream.py -m export -sn 2024-03-01-161745ce881d60-8cc7-4110-a268-c5298d2b165b -f coffee_example.json
```

```bash
python access-stream.py
```