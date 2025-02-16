from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")

def generate_embedding(text):
    """
    Converts text into a numerical embedding vector.
    """
    return model.encode(text).tolist()