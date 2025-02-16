# document_parser

### Explanation of `upload_document` Logic

1. **Read File** → Writes the uploaded file to `/tmp/<filename>`.
2. **Folder UUID** → Creates a new folder in GCS (like `abcd1234/`) to store chunks & the manifest.
3. **Check Size** → If the file is  **<= 50MB** , we keep it as one chunk. Otherwise, we call `split_file_into_chunks(...)`.
4. **Upload** → We call `upload_to_gcs(chunk_paths, SOURCE_BUCKET, folder_prefix=f"{folder_uuid}/")`, storing them as:gs://SOURCE_BUCKET/abcd1234/...
5. **Create Manifest** → `create_manifest` saves `manifest.json` with `num_chunks`, chunk filenames, and a `timestamp`.gs://SOURCE_BUCKET/abcd1234/manifest.json
6. **Cleanup** → Removes local temp files from `/tmp`.

### Example GCS Layout After Upload

##### **Case: Large File (90MB)** → Splits into 2 chunks

gs://SOURCE_BUCKET/
│── abcd1234/  # e.g. folder_uuid from the code
│   ├── `<filename>`.chunk_0
│   ├── `<filename>`.chunk_1
│   ├── manifest.json

##### **Case: Small File (45MB)** → Single chunk:

gs://SOURCE_BUCKET/
│── pqrs5678/
│   ├── `<filename>`
│   ├── manifest.json

##### The manifest might look like:

{
  "original_filename": "some_report.pdf",
  "num_chunks": 2,
  "chunk_files": [
    "`<filename>`.chunk_0",
    "`<filename>`.chunk_1"
  ],
  "timestamp": "2025-02-15T15:30:02Z"
}


### Run Locally (without Docker)

docker-compose build uploader_processor
docker-compose up -d uploader_processor

**Visit** : [http://localhost:8080/docs]() for Swagger UI.

**POST** to `/upload` with `multipart/form-data` → `file=@large.pdf`.

uvicorn main:app --host 0.0.0.0 --port 8080

upload a file

curl -X POST http://localhost:8080/upload -F "file=@sample.pdf"

## Deep Dive into Metadata Checkpointing & Retry/Exponential Backoff

### Source_bucket folder Layout

gs://SOURCE_BUCKET/
│── abcd-1234/  # Large document (90MB PDF, chunked)
│   ├── chunk_001.pdf
│   ├── chunk_002.pdf
│   ├── manifest.json  # Lists expected chunks

│── pqrs-2345/  # Small document (45MB PDF, single file)
│   ├── chunk_001.pdf
│   ├── manifest.json  # Only one chunk mentioned

│── xyz-5678/  # Large document (120MB, chunked)
│   ├── chunk_001.pdf
│   ├── chunk_002.pdf
│   ├── chunk_003.pdf
│   ├── manifest.json  # Lists all expected chunks

**Example manifest.json (for `abcd-1234`)**

{
    "num_chunks": 2,
    "chunk_files": ["chunk_001.pdf", "chunk_002.pdf"],
    "original_filename": "large_report.pdf",
    "file_size_mb": 90
}

**Example manifest.json (for `pqrs-2345`)**

{
    "num_chunks": 1,
    "chunk_files": ["chunk_001.pdf"],
    "original_filename": "small_report.pdf",
    "file_size_mb": 45
}

### OUTPUT Folder Layout

gs://OUTPUT_BUCKET/
│── structured_data/  # Stores extracted structured data
│   ├── abcd-1234/  # Unique folder for each document
│   │   ├── report_text.txt       # Full extracted text
│   │   ├── report_entities.json  # Key-value entities (invoice amounts, dates, etc.)
│   │   ├── report_tables.csv     # Extracted tables
│   │   ├── chunks/               # Stores chunked text files
│   │   │   ├── chunk_001.json
│   │   │   ├── chunk_002.json
│   │   │   ├── chunk_003.json
│   │   │   ├── chunks_metadata.json  # Checkpoint file (Stores chunking progress)
│   │   ├── embeddings/           # Stores vector embeddings
│   │   │   ├── chunk_001.npy
│   │   │   ├── chunk_002.npy
│   │   │   ├── embeddings_metadata.json  # Checkpoint file (Stores embedding progress)
│── metadata/  # Stores processing metadata (including LRO tracking)
│   ├── abcd-1234/
│   │   ├── report.json   # Tracks LRO status & final processing status

### Metadata Checkpointing → Tracks LRO Progress

* **Checkpointing** ensures that the system knows where it left off if there's an interruption (e.g., system crash, network failure).
* We store **LRO progress metadata** in a GCS  **metadata folder** .
* This helps us **resume from the last known state** instead of restarting from scratch.

#### Example Scenario:

##### Chunks for `report.pdf`

gs://SOURCE_BUCKET/abcd-1234/  contains following files

- report_chunk_1.pdf
- report_chunk_2.pdf
- report_chunk_3.pdf
- manifests_report.json

##### Metadata Stored in GCS:

gs://OUTPUT_BUCKET/metadata/abcd-1234/report.json

**File Content (`report.json`):**

{
    "lro_id": "projects/123456/locations/us/processors/67890/operations/98765",
    "status": "IN_PROGRESS",
    "last_checked": "2025-02-13T12:00:00Z"
}

#### Why is this Useful?

1. If the LRO fails halfway, we **retrieve the LRO ID** and  **resume processing** .
2. If GCP crashes, we **check this file** to know **which files were processed**
3. We avoid **duplicate processing** by checking status before restarting.

### Retry/Exponential Backoff

##### What is Exponential Backoff?

1. Instead of retrying **immediately** when something fails, we **increase the wait time** after each failure
2. This helps:
   * Avoid overwhelming the system with retries.
   * Allow the system time to **recover from transient errors** (network issues, API rate limits).
   * Prevent **wasting compute resources** on failures that might fix themselves.

##### Example Scenario:

Let's say we have  **3 chunked documents** :  `gs://SOURCE_BUCKET/abcd-1234/`

- report_chunk_1.pdf
- report_chunk_2.pdf
- report_chunk_3.pdf
- manifests_report.json

**Now, LRO Processing Starts, but It Fails!**

**First Attempt → FAIL**

* Waits **1 second** before retrying.

**Second Attempt → FAIL**

* Waits **2 seconds** before retrying.

**Third Attempt → FAIL**

* Waits **4 seconds** before retrying.

**Fourth Attempt → SUCCESS**

* The process resumes normally.

**How it Works in Code?**

```
from google.api_core.retry import Retry@Retry(
    initial=1.0,   # Start with a 1-second wait
    maximum=60.0,  # Maximum wait time of 60 seconds
    multiplier=2.0,  # Double the wait time after each failure
    deadline=600.0,  # Stop retrying after 10 minutes
    predicate=lambda e: isinstance(e, ServiceUnavailable)
)
def start_lro():
    return client.batch_process_documents(request)
```

##### **Why is this Useful?**

1. **If Document AI API is temporarily unavailable, it retries automatically.**
2. **Reduces failures due to transient network issues.**
3. **Saves money** → Instead of spamming API calls, it waits longer between retries.

### **Key Takeaways**

| Feature                          | What It Does                                                                                                                      |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| **Metadata Checkpointing** | Keeps track of**LRO status**in GCS, so if a failure occurs, it**resumes from last known state**instead of restarting. |
| **Exponential Backoff**    | Gradually increases retry time for failures, preventing excessive retries and**giving the system time to recover** .        |

## **1. What if the Cloud Function Fails Midway?**

Cloud Functions **are stateless** and have execution limits, so if a function  **crashes, times out, or fails due to API issues** , it  **must recover gracefully** .

Here’s how we ensure reliability:

### **A. Retrying Cloud Function Automatically**

Google Cloud Storage automatically **retries the function** if:

* A transient error occurs.
* The function crashes before it finishes.

**How retries work:**

* Google Cloud **automatically retries failed Cloud Functions** up to  **7 times** .
* The function should be **idempotent** (so it doesn’t restart work unnecessarily).

**Our function is idempotent because:**

* It  **checks metadata (`report.json`) before starting LRO** .
* If the LRO is  **already in progress** , it does **not** start a new one.

### **B. Tracking Failures in Metadata**

* **Every step updates `report.json` in the metadata folder.**
* If the function crashes, next time it runs, it:
  * Reads the  **last stored LRO ID** .
  * Checks if the LRO is  **still in progress or completed** .
  * Resumes processing instead of restarting.

## **Can Cloud Functions Handle Large Files (50GB+)?**

Cloud Functions are **not ideal for processing very large files** alone because:

1. **Cloud Function Timeout Limits**

* **Max execution time:** **60 minutes (Gen 2)**
* Large file processing might take **hours** → not suitable for long-running tasks.

2. **Memory & CPU Limits**

* **Max RAM:** **8GB**
* **Max concurrency:** **1000 instances**
* **Big files → high memory usage** . Cloud Functions might  **run out of RAM** .

3. **Better Alternative for Large Processing**

* Instead of Cloud Functions, **Cloud Run/kubernetes api or Dataflow** can be used.
* **Why?**
  **Cloud Run** allows **scaling up** and  **longer runtimes** .
  **Dataflow (Apache Beam)** is  **built for large-scale parallel processing** .

## PubSub approach rather than cloud function

**Flow in Practice**

1. **User Uploads File** to the GCS bucket.
2. GCS **publishes** an event to  **`myPubSubTopic`** .
3. **Pub/Sub** sends an **HTTP POST** to `https://my-k8s-lb.example.com/pubsub/push`.
4. The **Kubernetes Ingress** receives the request, routes it to a  **FastAPI pod** .
5. The **`pubsub_push`** route in **FastAPI** processes the message (decodes base64, gets file info).
6. **FastAPI** can **start LRO** (Document AI) or other logic.

### **GCS → Pub/Sub Notification**

1. **Enable GCS Pub/Sub notifications** on your bucket:
   * GCS can be configured to **send a Pub/Sub message** whenever a new file is uploaded.
   * This involves setting a **Pub/Sub topic** that GCS will publish to whenever an object finalize event occurs.
2. **Result** :

* Each **object upload** in the GCS bucket → **Pub/Sub** publishes an event with JSON data about the object.

### Pub/Sub Topic → Push Subscription

* Create a **push subscription** on that Pub/Sub topic.
  * A **push subscription** instructs Pub/Sub to **HTTP POST** incoming messages to a specified endpoint.
* **Specify an HTTPS endpoint** for the push:
  * This endpoint must be **publicly reachable** and **secured** (HTTPS recommended).
  * In GKE, you typically create an **Ingress** or **HTTP Load Balancer** that exposes your FastAPI service externally.

# Chunking Service

### Output Folder Layout

gs://OUTPUT_BUCKET/
│── structured_data/  # Stores extracted structured data
│   ├── abcd-1234/  # Unique folder for each document
│   │   ├── report_text.txt       # Full extracted text
│   │   ├── report_entities.json  # Key-value entities (invoice amounts, dates, etc.)
│   │   ├── report_tables.csv     # Extracted tables
│   │   ├── chunks/
│   │   │   ├── chunk_001.json
│   │   │   ├── chunk_002.json
│   │   │   ├── chunk_003.json
│   │   │   ├── chunks_metadata.json  # Checkpoint file
│   │   │   ├── chunks_errors.json    # Failed chunk logs
│   │   ├── embeddings/           # Stores vector embeddings
│   │   │   ├── chunk_001.npy
│   │   │   ├── chunk_002.npy
│   │   │   ├── embeddings_metadata.json  # Checkpoint file (Stores embedding progress)
│── metadata/  # Stores processing metadata (including LRO tracking)
│   ├── abcd-1234/
│   │   ├── report.json   # Tracks LRO status & final processing status

### Chunking Metadata Checkpointing

**Purpose** : Tracks which chunks were created, so that if the process fails, it does not reprocess already completed chunks.

* **Checkpointing** ensures that the system knows where it left off if there's an interruption (e.g., system crash, network failure).

#### **Checkpoint after 1st run**

###### **chunks_metadata.json**

{
    "processed_chunks": ["chunk_001.json", "chunk_003.json"]
}

###### **chunks_errors.json**

{
    "failed_chunks": [
        {"chunk": "chunk_002.json", "error": "TimeoutError"}
    ]
}

#### **Second Run (Retries & Completes)**

**Reads `chunks_metadata.json`** → Skips `chunk_001.json` & `chunk_003.json`
**Retries `chunk_002.json`** →  Success → Removes from error log
**Processes `chunk_004.json`** →  Success → Removes from error log
**Processing complete!**

###### Final Checkpoint (`chunks_metadata.json`):

**chunks_errors.json**

{
    "processed_chunks": [
        "chunk_001.json",
        "chunk_002.json",
        "chunk_003.json",
        "chunk_004.json"
    ]
}

###### Final Error Log (`chunks_errors.json`):

{
    "failed_chunks": []
}

**How it works:**

* If a chunk  **fails** , it's stored in `"failed_chunks"` with an  **error message & timestamp** .
* The next run  **only processes failed chunks** , ensuring fault tolerance.
* If the error is transient (e.g.,  **Timeout** ), the **@Retry decorator automatically retries** before logging.  will **only process** `chunk_002.json` & `chunk_005.json` instead of restarting.

### **Why This is Perfect for Large-Scale Processing**

 **Fault-Tolerant** → If a function crashes, it  **resumes from last checkpoint** .
 **Automatic Retries** → Only **failed chunks** are retried,  **saving processing time** .
 **Parallel Processing** → If multiple functions process different documents, each has  **its own checkpoint file** .

### Embedding Metadata Checkpointing

**embeddings_metadata.json**

{
    "total_chunks": 3,
    "processed_embeddings": [
        "chunk_001.npy",
        "chunk_002.npy"
    ],
    "last_processed": "2025-02-13T12:30:00Z"
}

**Logic**

 The embedder first checks this file  **before processing any chunk** .
 If a chunk is already embedded, it  **skips that chunk** .
 If an embedding fails  **midway** , the function can resume from the last unprocessed chunk.

# Embedding Service

### Updated GCS Folder Structure , Before Embedding

gs://OUTPUT_BUCKET/structured_data/
│── abcd-1234/
│   ├── chunks/
│   │   ├── chunk_001.json
│   │   ├── chunk_002.json
│   │   ├── chunk_003.json

### After Successful Embedding:

gs://OUTPUT_BUCKET/structured_data/
│── abcd-1234/
│   ├── chunks/
│   │   ├── chunk_001.json
│   │   ├── chunk_002.json
│   │   ├── chunk_003.json
│   ├── embeddings/
│   │   ├── chunk_001.npy
│   │   ├── chunk_002.npy
│   │   ├── chunk_003.npy
│   │   ├── embeddings_metadata.json  # Tracks completed embeddings
│   │   ├── embeddings_errors.json    # Tracks failed embeddings

### Updated Checkpointing Files

**`embeddings_metadata.json`** (Tracks Successfully Processed Chunks)

{
    "processed_chunks": [
        "chunk_001.npy",
        "chunk_002.npy"
    ]
}

**`embeddings_errors.json`** (Tracks Failed Chunks for Automatic Retry)

{
    "failed_chunks": [
        {
            "chunk": "chunk_003.json",
            "error": "Timeout Error",
            "timestamp": "2025-02-14T12:34:56Z"
        }
    ]
}

### How the Function Handles Failures

#####  **If the CF Crashes:**

1. It **skips already processed embeddings** by checking `embeddings_metadata.json`.
2. It **resumes from where it left off** instead of restarting from the beginning.

#####  **If an Embedding Fails:**

1. The failed chunk is  **logged in `embeddings_errors.json`** .
2. The next execution will  **retry only failed chunks** , not everything.

#####  **If Everything is Successfully Embedded:**

1. The function  **exits without retrying anything** .
2. The `embeddings_metadata.json` file is  **updated with processed chunks** .
