from chromadb import PersistentClient
import json

def batch_iterable(iterable, batch_size):
    for i in range(0, len(iterable), batch_size):
        yield iterable[i:i + batch_size]

with open('ini-chromadb.json', 'r', encoding='utf-8') as archivo:
    total = json.load(archivo)

client = PersistentClient(path="/home/luisazanavega/chroma_db/chroma_db")
collection = client.get_or_create_collection("materias_final_prueba")

batch_size = 4000  # seguro, menor que el límite

for i, (
    id_batch,
    doc_batch,
    emb_batch,
    meta_batch
) in enumerate(zip(
    batch_iterable(total['ids'], batch_size),
    batch_iterable(total['documents'], batch_size),
    batch_iterable(total['embeddings'], batch_size),
    batch_iterable(total['metadatos'], batch_size)
)):
    print(f"Cargando batch {i + 1}")
    collection.add(
        ids=id_batch,
        documents=doc_batch,
        embeddings=emb_batch,
        metadatas=meta_batch
    )
