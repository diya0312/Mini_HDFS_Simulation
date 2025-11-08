## 9_Project1_BD
**Big Data Mini Project** – Distributed File System Simulation  
This project simulates the working of an HDFS with one NameNode and two DataNodes. The system supports file upload, chunking, replication, and download.

## Team Details
Developed by:
- Dhanya Prabhu - PES2UG23CS169 [Client]
- Diya D Bhat - PES2UG23CS183 [Namenode]
- Delisha Riyona Dsouza - PES2UG23CS166 [Datanode 0]
- Deesha C - PES2UG23CS165 [Datanode 1]

## Project Overview

### Components
1. **NameNode**
   - Acts as the central metadata server.
   - Manages file-to-chunk mappings and DataNode assignments.
   - Coordinates file uploads, downloads, and deletions.

2. **DataNodes**
   - Receive, store, and serve file chunks.
   - Send periodic heartbeats to the NameNode.
   - Secondary DataNode handles **replication and redundancy**.

3. **Client**
   - Provides both CLI and web-based interfaces for users.
   - Uploads/downloads files by communicating with the NameNode.
   - Handles interaction and display of stored file information.

## Run the project
### Step 1: Clone the Repository  
  ```bash
  git clone https://github.com/pes2ug23cs169/9_Project1_BD.git
  cd 9_Project1_BD
  ```
### Step 2: Start the NameNode

The Namenode acts as the central controller of the Distributed File System. It maintains the file-to-chunk mappings, assigns chunks to DataNodes, tracks replication, node health and also includes a visual dashboard to monitor Datanodes and stored chunks in real time.

Start the namenode using this below command and run the **namenode.py** file present in the namenode folder.
```bash
python3 namenode.py
```

- Once started, the Namenode:

   - Begins listening on the assigned host and port.
   - Waits for Datanodes to register and send periodic heartbeats.
   - Displays all active Datanodes and stored file chunks on the visual dashboard.
   - Handles all client requests such as:
        - Uploading files (chunk mapping and replication)
        - Downloading files
        - Deleting files
        - Replicating chunks automatically if a Datanode fails
   - Maintains a metadata file **(metadata.json)** that stores:
        - File-to-chunk mappings
        - Chunk-to-DataNode assignments
        - Replication details and current node states
     > This file is automatically updated whenever new files are uploaded, deleted, or replicated, ensuring persistence even after restarts.

- Now open your browser and visit **http://<namenode_ip>:5000/** (Example: http://10.144.198.253:5000/ or http://127.0.0.1:5000/) in your browser to view the Namenode Dashboard,
where you can see node status, stored chunks, and replication status live.

### Step 3: Start the datanode 0
The datanode 0 is responsible for 
   - Chunk reception & storage 
   - Heartbeat sender thread & communication stability 
   - Retrieval logic for client downloads 
   - Data integrity checks (no corruption on retrieval)
   - Logging & error handling

This module implements a DataNode for a simplified HDFS-like Distributed File System 
It is responsible for storing, retrieving, replicating, verifying, and deleting data chunks, and for communicating with the NameNode through periodic heartbeats and registration updates.

---

##  Overview

Each **DataNode**:
- Runs as a **Flask web service**.
- Stores chunks of data locally on disk.
- Responds to client or NameNode requests for storing, retrieving, replicating, verifying, or deleting chunks.
- Sends heartbeat signals periodically to the NameNode to indicate that it is alive.
- Maintains checksums (SHA-256) to ensure data integrity.
- Registers every new chunk with the NameNode after successful storage.

---

##  Key Features

| Feature | Description |
|----------|--------------|
| **Chunk Storage** | Receives file chunks, stores them locally, and registers with the NameNode. |
| **Chunk Retrieval** | Serves requested chunks in base64 format, verifying integrity via checksum. |
| **Replication** | Sends stored chunks to another DataNode when instructed by the NameNode. |
| **Heartbeat** | Periodically notifies the NameNode that this DataNode is active. |
| **Integrity Verification** | Verifies that stored data matches its checksum hash. |
| **Chunk Deletion** | Deletes chunks and associated checksum files safely. |

---

##  Command-Line Arguments

| Argument | Description | Example |
|-----------|--------------|----------|
| `--id` | Unique DataNode identifier | `--id dn0` |
| `--port` | Port number for this DataNode’s Flask app | `--port 6001` |
| `--namenode` | Base URL of the NameNode | `--namenode http://10.144.198.253:5000` |
| `--data_dir` | (Optional) Custom directory to store chunks | `--data_dir ./data_dn0` |

If `--data_dir` is not provided, it defaults to `./data_<id>`.

---

You can start the datanode 0 using the command
``` bash
python3 datanode0.py --id dn0 --port 8001 --namenode http://10.144.198.253:5000 --data_dir ./data_dn0
```
- here the namenode ip is 10.144.198.253 with port number 5000

### Step 4: Start the datanode 1
The datanode 1 is responsible for 
   - Chunk replication & verification
   - Heartbeat sender thread & connectivity
   - Serving replica chunks to client
   - Data validation & checksum consistency
   - Robustness & recovery during node failure tests

## Key Features
| Feature | Description |
|----------|-------------|
| **Chunk Replication** | Receives chunks from other datanodes and stores them with checksum verification. |
| **Checksum Validation** | Verifies integrity using SHA-256 to prevent corruption. |
| **Heartbeat Thread** | Sends regular heartbeats to Namenode to indicate node health. |
| **Recovery Mechanism** | Automatically fetches missing chunks from peers if the node restarts or detects loss. |
| **REST API Endpoints** | Provides `/store_chunk`, `/replicate_chunk`, `/get_chunk`, `/verify_chunk`, and `/delete_chunk` endpoints for communication. |
| **Threaded Execution** | Uses background threads for heartbeat and recovery parallelism. |


## Threads
- **Heartbeat Thread:**  
  Periodically sends a status signal (`/heartbeat`) to the Namenode.  
- **Recovery Thread:**  
  Checks for missing chunks and restores them automatically from other datanodes.
  

You can start the datanode 1 using the command
``` bash
python3 datanode2.py --id dn1 --port 8002 --namenode http://10.144.198.253:5000 --data_dir ./data_dn1
```
- here the namenode ip is 10.144.198.253 with port number 5000
  
### Step 5: Client interface  
You can use either the Web Interface or the Command Line Interface (CLI).

- Option 1 – Web Interface
    ```bash
    python3 client_web.py
    ```
  - Now open your browser and visit: **http://<client_ip>:5050**
  Example: http://10.144.141.149:5050
  - From the web UI, you can:
    1. Upload files
    2. Download files
    3. Delete files
    4. View the chunk distribution in the dashboard section
- Option 2 – Command Line Interface (CLI)
  1. Create a file sample.txt (You can even upload any other file)
     ```bash
     echo "This is a sample file for upload to test the mini HDFS system" > sample.txt
     ```
     > **Note:** It is better to have a file which is above 50bytes to see the chunk distributions properly.
  2. Upload a file
     ```bash
     python3 client.py upload sample.txt
     ```
  3. Download a file
     ```bash
     python3 client.py download sample.txt
     ```
  4. Delete a file
     ```bash
     python3 client.py delete sample.txt
     ```
