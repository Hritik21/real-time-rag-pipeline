# Real-Time Event-Driven RAG Pipeline

This repository demonstrates a local, low-latency, real-time Retrieval-Augmented Generation (RAG) architecture. Rather than relying on scheduled batch ETL jobs to update a vector database, this pipeline utilizes Change Data Capture (CDC) to stream database mutations instantly into a semantic search vector store.

The entire stack is optimized to run locally on Apple Silicon (M-series) with a highly constrained memory footprint.

---

## 🏗️ Architecture

1. **Source Database (PostgreSQL)**  
   Configured with `wal_level = logical`. Acts as the active data generator.

2. **CDC Engine (Debezium)**  
   Trails the Postgres Write-Ahead Log (WAL), captures row-level changes in real time, and publishes them.

3. **Event Broker (Apache Kafka)**  
   Serves as the immutable, distributed messaging nervous system.

4. **Stream Processing (PySpark Structured Streaming)**  
   Consumes raw binary JSON events from Kafka, strips metadata, and isolates the payload.

5. **Local AI Brain (Ollama)**  
   Runs the highly optimized `nomic-embed-text` model locally to convert raw text into 768-dimensional embeddings on the fly.

6. **Vector Sink (Qdrant)**  
   A custom micro-batch Python function sinks the vectorized data into Qdrant for immediate semantic retrieval.

---

## 📁 Repository Structure

```text
.
├── main/
│   └── spark_rag_consumer.py   # PySpark streaming job and Qdrant sink logic
├── qdrant_storage/             # Local volume mapping for persistent vector data
├── search/
│   └── search.py               # CLI script to test semantic search
├── docker-compose.yml          # Infrastructure (Postgres, Zookeeper, Kafka, Debezium, Qdrant)
└── README.md
```

---

## ⚙️ Prerequisites

- **Hardware:** Tested on macOS (M2 Apple Silicon, 8GB Unified Memory)
- **Docker Desktop:** Memory limit constrained to ~4GB
- **Python:** Version 3.10+
- **Java:** OpenJDK 17 (Required for modern PySpark)
- **LLM Engine:** [Ollama](https://ollama.ai/) installed locally

---

## 🚀 Setup & Execution

### 1. Initialize the Infrastructure

Spin up PostgreSQL, Zookeeper, Kafka, Debezium, and Qdrant in detached mode.

```bash
docker-compose up -d
```

---

### 2. Configure the Embedding Model

Pull the lightweight embedding model into your local Ollama instance.

```bash
ollama pull nomic-embed-text
```

---

### 3. Configure the Python Environment

Create an isolated environment and install the required dependencies.

> **Note:** PySpark is pinned to **3.5.1** to maintain compatibility with Scala **2.12** and the Kafka SQL connector.

```bash
python3 -m venv venv

source venv/bin/activate

pip install pyspark==3.5.1 requests qdrant-client
```

---

### 4. Patch Network Routing *(macOS Specific)*

If you encounter IPv6 routing exceptions (`java.net.SocketException`) when PySpark attempts to download Maven packages, force the JVM to use IPv4.

```bash
export _JAVA_OPTIONS="-Djava.net.preferIPv4Stack=true"

export SPARK_LOCAL_IP="127.0.0.1"
```

---

### 5. Start the Streaming Consumer

Boot the PySpark streaming engine. This process will run continuously, polling the Kafka topic for new database events.

```bash
python3 main/spark_rag_consumer.py
```

---

### 6. Verify the Pipeline

Connect to the local PostgreSQL instance.

| Property | Value |
|----------|-------|
| Port | `5432` |
| Username | `postgres` |
| Password | `postgres` |

Insert a new record into the `articles` table.

```sql
INSERT INTO articles (title, content)
VALUES (
    'Event Streaming',
    'Kafka processes data streams in real-time with low latency.'
);
```

After inserting the record:

- Check your active terminal—PySpark will log a successful micro-batch push to Qdrant within milliseconds.
- Run the semantic search script to query the vector database.

```bash
python3 search/search.py
```

---

## 🧠 Engineering & Technical Notes

### Memory Optimization

The PySpark `SparkSession` is strictly capped at **1 GB** for both driver and executor memory. This prevents heavy JVM swapping and allows the entire enterprise stack to run seamlessly on an **8 GB** machine alongside Docker.

### Serialization Bypass

The Ollama HTTP request logic is executed purely within standard Python inside the `foreachBatch` sink rather than using a PySpark UDF. This completely bypasses `cloudpickle` stack overflow bugs common in newer Python versions (3.12+) when attempting to serialize deep network dependencies like `requests`.

---

## 👤 Author

**Hritik** | Data Engineer