def chunk_text(text, chunk_size=512):
    """
    Splits text into smaller chunks of `chunk_size` words.
    """
    words = text.split()
    return [" ".join(words[i : i + chunk_size]) for i in range(0, len(words), chunk_size)]
