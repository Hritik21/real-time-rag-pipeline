# Real-Time Event-Driven RAG Pipeline

> A production-inspired event-driven pipeline that continuously synchronizes PostgreSQL data with a vector database using Change Data Capture (CDC), Apache Kafka, PySpark Structured Streaming, Ollama, and Qdrant.

Traditional RAG systems typically rely on scheduled ETL jobs to regenerate embeddings, introducing synchronization delays and unnecessary recomputation. This project demonstrates an event-driven alternative where every database mutation is captured directly from PostgreSQL's Write-Ahead Log (WAL) and propagated through a streaming pipeline, enabling near real-time semantic indexing.

---

# 🚀 Highlights

- Real-time Change Data Capture (CDC) using PostgreSQL WAL
- Event-driven architecture with Debezium and Apache Kafka
- Continuous processing using PySpark Structured Streaming
- Local embedding generation with Ollama (`nomic-embed-text`)
- Automatic synchronization with Qdrant Vector Database
- Fully local deployment optimized for Apple Silicon
- No external LLM or embedding APIs required

---

# 🏗️ Architecture

> **Note:** *Architecture diagram can be added here.*

```text
             PostgreSQL
                  │
      Logical Replication (WAL)
                  │
                  ▼
             Debezium CDC
                  │
                  ▼
            Apache Kafka
                  │
                  ▼
     PySpark Structured Streaming
                  │
      Parse → Embed → foreachBatch
                  │
                  ▼
       Ollama (nomic-embed-text)
                  │
                  ▼
              Qdrant
                  │
                  ▼
          Semantic Search
```

---

# ⚙️ Pipeline Overview

### **Source Database (PostgreSQL)**

Configured with:

```text
wal_level = logical
```

Acts as the transactional source of truth.

---

### **CDC Engine (Debezium)**

Continuously monitors PostgreSQL's Write-Ahead Log (WAL) and converts row-level database mutations into Kafka events without polling the database.

---

### **Event Broker (Apache Kafka)**

Acts as the immutable messaging layer that decouples producers from downstream consumers.

---

### **Stream Processing (PySpark Structured Streaming)**

Consumes CDC events from Kafka and performs:

- JSON parsing
- Metadata removal
- Payload extraction
- Micro-batch processing
- Embedding generation
- Vector upsert into Qdrant

---

### **Local AI (Ollama)**

Runs the lightweight `nomic-embed-text` embedding model locally to generate 768-dimensional embeddings without relying on external APIs.

---

### **Vector Database (Qdrant)**

Stores vector embeddings and enables low-latency semantic retrieval.

---

# 📁 Repository Structure

```text
.
├── main/
│   └── spark_rag_consumer.py     # Spark streaming job and Qdrant sink
│
├── search/
│   └── search.py                 # Semantic search CLI
│
├── docker-compose.yml            # PostgreSQL, Kafka, Debezium, Qdrant
├── README.md
└── .gitignore
```

---

# 🔄 Data Flow

```text
INSERT INTO PostgreSQL

        │

        ▼

 Write-Ahead Log (WAL)

        │

        ▼

    Debezium CDC

        │

        ▼

     Kafka Topic

        │

        ▼

Spark Structured Streaming

        │

        ▼

Generate Embeddings

        │

        ▼

Qdrant

        │

        ▼

Semantic Search
```

---

# 💡 Engineering Decisions

## Why Change Data Capture?

Instead of periodically polling PostgreSQL using queries like:

```sql
SELECT *
FROM articles
WHERE updated_at > ...
```

Debezium reads directly from PostgreSQL's Write-Ahead Log (WAL), enabling near real-time synchronization.

**Benefits**

- Lower database load
- Low-latency event propagation
- Event-driven synchronization
- Automatic capture of every database mutation

---

## Why Apache Kafka?

Kafka serves as the event backbone of the architecture, decoupling producers and consumers while allowing additional downstream systems to subscribe without impacting the source database.

---

## Why PySpark Structured Streaming?

Although this workload could be processed by a lightweight Kafka consumer, Spark Structured Streaming was intentionally chosen to demonstrate production-grade streaming patterns such as:

- Fault tolerance
- Checkpointing
- Micro-batch execution
- Horizontal scalability

The architecture can scale to higher event throughput with minimal code changes.

---

## Why Ollama?

Embedding generation runs entirely on the local machine.

**Benefits**

- No API cost
- Low latency
- Offline execution
- Improved data privacy

---

## Why `foreachBatch` Instead of a Spark UDF?

Embedding generation is executed inside the `foreachBatch` sink instead of a Spark UDF. This avoids Python serialization (`cloudpickle`) issues commonly encountered when network-heavy dependencies such as `requests` are executed inside distributed Spark workers.

---

## Memory Optimization

The Spark application is intentionally configured with:

```text
Driver Memory     : 1 GB
Executor Memory   : 1 GB
```

allowing the complete infrastructure to run comfortably on an Apple Silicon laptop with 8 GB unified memory.

---

# ⚙️ Prerequisites

- macOS (Apple Silicon recommended)
- Docker Desktop
- Python 3.10+
- OpenJDK 17
- Ollama

---

# 🚀 Setup

### 1. Start Infrastructure

```bash
docker compose up -d
```

---

### 2. Pull the Embedding Model

```bash
ollama pull nomic-embed-text
```

---

### 3. Create the Python Environment

```bash
python3 -m venv venv

source venv/bin/activate

pip install pyspark==3.5.1 requests qdrant-client
```

---

### 4. macOS Networking Fix

```bash
export _JAVA_OPTIONS="-Djava.net.preferIPv4Stack=true"

export SPARK_LOCAL_IP=127.0.0.1
```

---

### 5. Start the Streaming Consumer

```bash
python main/spark_rag_consumer.py
```

---

# 🧪 Verify the Pipeline

Insert a record into PostgreSQL.

```sql
INSERT INTO articles (title, content)
VALUES (
    'Event Streaming',
    'Kafka processes data streams in real-time with low latency.'
);
```

The record flows through:

```text
PostgreSQL

↓

Debezium

↓

Kafka

↓

Spark Structured Streaming

↓

Ollama

↓

Qdrant

↓

Semantic Search
```

Run the semantic search client.

```bash
python search/search.py
```

---

# 📊 Sample Streaming Logs

The streaming consumer initializes the Spark session, consumes CDC events from Kafka, generates embeddings using Ollama, and continuously pushes vectors into Qdrant through Spark micro-batches.

```text
26/07/19 14:19:19 WARN NativeCodeLoader:
Unable to load native-hadoop library for your platform...
using builtin-java classes where applicable.

Setting default log level to "WARN".

26/07/19 14:19:21 WARN ResolveWriteToStream:
Temporary checkpoint location created.

26/07/19 14:19:21 WARN ResolveWriteToStream:
spark.sql.adaptive.enabled is not supported in
streaming DataFrames/Datasets and will be disabled.

🚀 Batch 1: Embedded & pushed 1 vectors to Qdrant!

🚀 Batch 2: Embedded & pushed 4 vectors to Qdrant!
```

The logs confirm that Spark Structured Streaming successfully consumes Kafka events, generates embeddings locally, and incrementally synchronizes vectors into Qdrant.

---

# 🔍 Sample Search Output

> *Run the following command after inserting records into PostgreSQL:*

```bash
python search/search.py
```

> **Search Output**

```text
🔍 Searching for: 'What are the benefits of real-time data streaming over batch?'

🎯 Match Score: 0.6827
📌 Title: AI in Data Engineering
📄 Content: Streaming CDC architectures reduce latency compared to batch ETL pipelines.
--------------------------------------------------
🎯 Match Score: 0.6200
📌 Title: The Evolution of CDC
📄 Content: Change Data Capture ensures that downstream systems stay in perfect sync with the primary database by streaming transaction logs.
--------------------------------------------------
```

This verifies that newly inserted PostgreSQL records become searchable shortly after flowing through the CDC → Kafka → Spark → Ollama → Qdrant pipeline.

---

# 📈 Future Improvements

- Support UPDATE and DELETE events
- Batch embedding generation
- Metadata filtering
- Hybrid keyword + vector search
- FastAPI search service
- Kubernetes deployment
- Monitoring with Prometheus and Grafana

---

# 👨‍💻 Author

**Hritik Maheshwari**

Data Engineer | Streaming Systems | Distributed Data Platforms | AI Infrastructure