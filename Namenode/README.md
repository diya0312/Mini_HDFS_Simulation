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
   - Includes visual dashboard to check node health and files stored. 

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

- Now open your browser and visit **http://<namenode_ip>:5000/** (Example: http://10.144.198.253:5000/) in your browser to view the Namenode Dashboard,
where you can see node status, stored chunks, and replication status live.

### Step 4: Client interface  
You can use either the Web Interface or the Command Line Interface (CLI).

- Option 1 – Web Interface
    ```bash
    python3 client_web.py
    ```
  - Now open your browser and visit: http://<client_ip>:5050
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

