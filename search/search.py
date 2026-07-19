import requests
from qdrant_client import QdrantClient

def get_query_embedding(text):
    """Hits your local Ollama to vectorize the search query."""
    url = "http://localhost:11434/api/embeddings"
    payload = {
        "model": "nomic-embed-text",
        "prompt": text
    }
    response = requests.post(url, json=payload)
    return response.json().get("embedding", [])

def main():
    # 1. The natural language question you want to ask your database
    query_text = "What are the benefits of real-time data streaming over batch?"
    print(f"🔍 Searching for: '{query_text}'\n")

    # 2. Convert the question into a vector
    query_vector = get_query_embedding(query_text)

    # 3. Connect to Qdrant
    client = QdrantClient(host="localhost", port=6333)
    
    # 4. UPDATED: Use query_points instead of the deprecated search method
    response = client.query_points(
        collection_name="articles_collection",
        query=query_vector,
        limit=2  # Return the top 2 closest matches
    )

    # 5. Display the results by looping through the .points attribute
    for hit in response.points:
        print(f"🎯 Match Score: {hit.score:.4f}")
        print(f"📌 Title: {hit.payload['title']}")
        print(f"📄 Content: {hit.payload['content']}")
        print("-" * 50)

if __name__ == "__main__":
    main()