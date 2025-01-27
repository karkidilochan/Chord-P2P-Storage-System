# Distributed Peer-to-Peer System Using Chord Protocol 🚀

This project implements a distributed peer-to-peer (P2P) network using the **Chord Protocol**, which provides efficient data storage and retrieval in a scalable, decentralized system. The project demonstrates key distributed systems concepts, such as consistent hashing, fault tolerance, and routing efficiency.

---

## Features 🛠️

- **Peer-to-Peer Network**:
  - Nodes organized in a ring topology using consistent hashing.
  - Each peer maintains a **Finger Table** for efficient routing.
- **Discovery Node**:
  - Facilitates new peer registration.
  - Provides information about active peers.
- **File Storage**:
  - Files are stored on the peer responsible for their hash using consistent hashing.
  - Dynamic file migration when nodes join or leave.
- **Efficient Routing**:
  - Uses the Finger Table to locate peers and files in `O(log N)` hops.
- **Fault Tolerance**:
  - Graceful node exit with file migration and routing table updates.

---

## Project Structure 📂

```plaintext
src/
├── distributed/
│   ├── chord/
│   │   ├── Discovery.java       # Discovery node logic
│   │   ├── Peer.java            # Peer node logic
│   │   ├── FingerTable.java     # Finger table implementation
│   │   ├── FileHandler.java     # File storage and transfer logic
│   │   ├── NodeUtils.java       # Utility methods for node operations
```

---
## System Components 🏗️

### 1. Discovery Node
- Manages peer registration and maintains a list of active peers.
- Provides:
  - Random peer lookup for new nodes.
  - List of active peers.

### 2. Peer Nodes
- Each peer maintains:
  - **Predecessor** and **Successor** links.
  - A **Finger Table** for routing.
  - A local storage directory for files.
- Dynamically updates its Finger Table and migrates files when nodes join/leave.

---

## Commands and Usage 🖥️

### Starting the System
1. **Start the Discovery Node**:
   ```bash
   java distributed.chord.Discovery <portNum>
   ```

2. **Start a Peer Node**:
   ```bash
   java distributed.chord.Peer <discovery-ip> <discovery-port>
   ```

---

### Peer Node Commands
- **Neighbors**:
  Prints predecessor and successor information:
  ```plaintext
  predecessor: <peerID> <ip>:<port>
  successor: <peerID> <ip>:<port>
  ```

- **Finger Table**:
  Prints the peer's routing table:
  ```plaintext
  <index> <peerID>
  ```

- **Files**:
  Lists files stored on the peer:
  ```plaintext
  <file-name> <hash-code>
  ```

- **Upload a File**:
  Uploads a file to the Chord system:
  ```bash
  upload <file-path>
  ```

- **Download a File**:
  Retrieves a file from the system:
  ```bash
  download <file-name>
  ```

- **Exit**:
  Gracefully exits the system, migrating files and updating the network:
  ```bash
  exit
  ```

---

### Example Workflow
1. **Start the discovery node**:
   ```bash
   java distributed.chord.Discovery 8080
   ```

2. **Start a peer node**:
   ```bash
   java distributed.chord.Peer localhost 8080
   ```

3. **Upload a file**:
   ```bash
   upload work/project/my_song.mp3
   ```

4. **Download a file**:
   ```bash
   download my_song.mp3
   ```

---


## Highlights 🌟

- Implements a scalable P2P system using the **Chord Protocol**.
- Demonstrates efficient file storage and retrieval with consistent hashing.
- Handles node churn dynamically by updating routing tables and migrating data.
- Efficiently routes requests in `O(log N)` hops.

---

## Requirements 📋

- **Java**: JDK 8 or higher
- **Build Tool**: Gradle

---

## Author ✍️

Designed as a portfolio project showcasing distributed systems expertise, consistent hashing, and fault-tolerant architectures.

---

### 📜 License
This project is licensed under the MIT License.
```

You can use this file directly for your GitHub repository! Let me know if there are further customizations or additional sections you'd like to include. 😊