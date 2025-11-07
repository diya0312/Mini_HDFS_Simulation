## 9_Project1_BD
**Big Data Mini Project** – Distributed File System Simulation  
This project simulates the working of an HDFS with one NameNode and two DataNodes. The system supports file upload, chunking, replication, and download.

## Components
- **NameNode:** Handles metadata, chunk distribution, and replication info.
- **DataNode0:** Receives and stores chunks, sends heartbeats.
- **DataNode1:** Acts as secondary DataNode for replication & redundancy.
- **Client:** Uploads, downloads, and deletes files by communicating with the NameNode.

