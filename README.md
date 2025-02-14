# document_parser

Run Locally (without Docker)

uvicorn main:app --host 0.0.0.0 --port 8080

upload a file

curl -X POST http://localhost:8080/upload -F "file=@sample.pdf"

## Deep Dive into Metadata Checkpointing & Retry/Exponential Backoff

### Metadata Checkpointing → Tracks LRO Progress

* **Checkpointing** ensures that the system knows where it left off if there's an interruption (e.g., system crash, network failure).
* We store **LRO progress metadata** in a GCS  **metadata folder** .
* This helps us **resume from the last known state** instead of restarting from scratch.

#### Example Scenario:

##### Chunks for `report.pdf`

gs://CHUNK_BUCKET/abcd-1234/  contains following files

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

Let's say we have  **3 chunked documents** :  `gs://CHUNK_BUCKET/abcd-1234/`

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

#####  **Why is this Useful?**

1. **If Document AI API is temporarily unavailable, it retries automatically.**
2. **Reduces failures due to transient network issues.**
3. **Saves money** → Instead of spamming API calls, it waits longer between retries.

### **Key Takeaways**

| Feature                          | What It Does                                                                                                                      |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| **Metadata Checkpointing** | Keeps track of**LRO status**in GCS, so if a failure occurs, it**resumes from last known state**instead of restarting. |
| **Exponential Backoff**    | Gradually increases retry time for failures, preventing excessive retries and**giving the system time to recover** .        |
