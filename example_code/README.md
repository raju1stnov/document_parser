# openai logic :-

## **Explanation of Key Functions**

1. **`read_manifest(upload_folder)`**
   * Reads `manifest.json` in `gs://SOURCE_BUCKET/myname/source_path/upload-<folder>/manifest.json`.
   * Example structure:

     {
     "no_files": "3",
     "file_names": ["my_test_pdf.pdf","my_test_doc.docx","my_test_excel.xl"],
     "timestamp": "2025-02-17T10:30:00Z"
     }
2. **`get_parse_manifest(upload_folder)`**
   * Reads `parse_manifest.json` in `gs://OUTPUT_BUCKET/myname/output_path/structured_data/<folder>/parse_manifest.json`.
   * If file doesn't exist, returns default:

     {
     "lro_id": null,
     "status": "NEW",
     "start_time": null,
     "end_time": null,
     "error": null
     }
3. **`save_parse_manifest(upload_folder, data)`**
   * Writes `parse_manifest.json` back to GCS so we keep track of the LRO status, error, start/end times, etc.
4. **`start_lro(upload_folder, manifest_data, parse_data)`**
   * **@Retry** auto-healing if there’s a transient error calling Document AI.
   * Builds a **batch process** request using the **file_names** from `manifest.json`.
   * Sets `parse_data["status"] = "IN_PROGRESS"` and writes out the new LRO ID.
5. **`check_lro_status(upload_folder, parse_data)`**
   * Reads LRO operation ID from `parse_data["lro_id"]`.
   * If the operation is done, sets `status = SUCCESS` or `FAILED`.
6. **`combine_outputs(upload_folder)`** (Optional)
   * In real usage, you’d parse the **JSON output** from Document AI (written into the same GCS folder) and unify it into a single `unstructured_data.txt`.
   * This snippet just uploads a placeholder.
7. **`main()`**
   * The main orchestration flow:
     * Reads `manifest.json` → `parse_manifest.json`
     * If `status == "NEW"`, call `start_lro(...)`.
     * If `status == "IN_PROGRESS"`, call `check_lro_status(...)`.
     * If `status == "SUCCESS"`, do final merges or skip.
     * If `status == "FAILED"`, handle error logic.

---

## **3) `parse_manifest.json` Example**

**After you start the LRO:**

{
  "lro_id": "projects/123456/locations/us/processors/6780/operations/987865",
  "status": "IN_PROGRESS",
  "start_time": "2025-02-17T17:45:09.123456",
  "end_time": null,
  "error": null
}

**When the LRO completes:**

{
  "lro_id": "projects/123456/locations/us/processors/6780/operations/987865",
  "status": "SUCCESS",
  "start_time": "2025-02-17T17:45:09.123456",
  "end_time": "2025-02-17T17:50:01.987654",
  "error": null
}

**If an error occurred:**

{
  "lro_id": "projects/123456/locations/us/processors/6780/operations/987865",
  "status": "FAILED",
  "start_time": "2025-02-17T17:45:09.123456",
  "end_time": "2025-02-17T17:47:11.000000",
  "error": "Service unavailable or user does not have permission"
}

---

## **4) Future Enhancements**

1. **Break large sets of files** into multiple batch calls if you have more than 50 docs.
2. **Parse the output JSON** from Document AI to unify text/annotations in `unstructured_data.txt`.
3. **Chunking & Embedding** : After storing `unstructured_data.txt`, you can trigger a chunking Cloud Function or embedding pipeline.

---

## **Conclusion**

You now have:

* **A fully working Python script** that **auto-retries** with `@Retry` for Document AI calls.
* **`parse_manifest.json`** to **resume** or skip already processed data.
* **`manifest.json`** from your source to ensure the correct files are included in the LRO.

# DEEPSEEK  logic :-
