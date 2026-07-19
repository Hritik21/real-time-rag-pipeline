import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance

# 1. Combined Embedding & Sink Function (No PySpark Pickling Required!)
def embed_and_write_to_qdrant(batch_df, batch_id):
    # Bring the micro-batch into standard Python memory
    rows = batch_df.collect()
    if not rows:
        return

    client = QdrantClient(host="localhost", port=6333)
    points = []
    
    # Process each row natively
    for row in rows:
        text = row.content
        if not text:
            continue
            
        # Call Ollama locally
        url = "http://localhost:11434/api/embeddings"
        payload = {
            "model": "nomic-embed-text",
            "prompt": text
        }
        
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            vector = response.json().get("embedding", [])
            
            # If Ollama returns the successful 768-dimensional vector, prep it for Qdrant
            if len(vector) == 768:
                points.append(
                    PointStruct(
                        id=row.id, 
                        vector=vector,
                        payload={
                            "title": row.title,
                            "content": row.content
                        }
                    )
                )
        except Exception as e:
            print(f"Ollama API call failed for row {row.id}: {e}")
            
    # Push the vectorized batch to Qdrant
    if points:
        client.upsert(
            collection_name="articles_collection",
            points=points
        )
        print(f"🚀 Batch {batch_id}: Embedded & pushed {len(points)} vectors to Qdrant!")

def main():
    spark = SparkSession.builder \
        .appName("Kafka-Postgres-CDC-RAG") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Setup Qdrant Collection
    qdrant = QdrantClient(host="localhost", port=6333)
    if not qdrant.collection_exists("articles_collection"):
        qdrant.create_collection(
            collection_name="articles_collection",
            vectors_config=VectorParams(size=768, distance=Distance.COSINE),
        )
        print("Created new Qdrant collection: articles_collection")

    # 3. Define Schemas
    article_payload_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("content", StringType(), True)
    ])

    debezium_schema = StructType([
        StructField("payload", StructType([
            StructField("after", article_payload_schema, True)
        ]), True)
    ])

    # 4. Read from Kafka
    raw_kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "pg-server.public.articles") \
        .load()

    # 5. Parse JSON
    parsed_stream = raw_kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), debezium_schema).alias("data")) \
        .select("data.payload.after.*") \
        .filter(col("id").isNotNull())

    # 6. Stream directly to our custom Python function
    query = parsed_stream.writeStream \
        .foreachBatch(embed_and_write_to_qdrant) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()