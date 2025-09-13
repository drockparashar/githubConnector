# Key Engineering Challenges & Solutions

This document outlines key engineering challenges encountered and their respective solutions and outcomes.

---

**1. Daft DataFrame Parquet File Writing on Windows**

- **Challenge:** The Daft’s `write_parquet` method created 0-byte, locked files on Windows, leading to file permission errors and preventing overwriting by pandas.
- **Solution:** Bypassed Daft’s parquet writing on Windows, opting for direct pandas writing.
- **Outcome:** Parquet files are now reliably created with data, enabling downstream processing.

**2. Transformer Parameter Handling**

- **Challenge:** Passing `None` values for `connection_name` and `connection_qualified_name` caused attribute errors during transformation.
- **Solution:** Implemented default values and validation for these parameters within the transformer logic.
- **Outcome:** The transformation step now functions without errors, regardless of input configuration completeness.

**3. Unicode Encoding Issues**

- **Challenge:** Non-ASCII characters in GitHub metadata led to encoding errors on Windows.
- **Solution:** Developed a text cleaning function to strip or replace problematic Unicode characters before file writing.
- **Outcome:** All metadata is safely encoded and written, preventing crashes due to encoding errors.

**4. Object Store Path Mismatches**

- **Challenge:** Incorrect object store prefix usage resulted in files not being found or uploaded to Atlan.
- **Solution:** Corrected the logic to use the appropriate object store prefix for migration and upload steps.
- **Outcome:** Transformed files are now correctly uploaded to Atlan storage.

**5. Workflow Argument and State Handling**

- **Challenge:** Race conditions and missing workflow arguments caused intermittent failures.
- **Solution:** Added retry logic and validation for workflow argument retrieval.
- **Outcome:** Workflow initialization is now robust and reliable.

**6. Workflow Startup Race Condition**

- **Challenge:** The workflow intermittently failed on startup with a "file not found" error when reading its configuration, due to a race condition.
- **Solution:** Overrode the default `get_workflow_args` activity and implemented a retry loop (five attempts, two-second delay) to wait for the configuration file.
- **Outcome:** The workflow startup process is now stable and resilient to object store latency.

**7. API Authentication and Frontend Feedback**

- **Challenge:** The "Test Connection" button in the UI falsely indicated "Connection Successful" even with an invalid GitHub token, due to backend and frontend error handling issues.
- **Solution:** Updated the backend to raise a `ValueError` on authentication failure and the frontend to properly check HTTP response status and display error messages.
- **Outcome:** The UI now accurately reflects authentication status and provides clear feedback to users.

**8. Inconsistent API Payloads**

- **Challenge:** The "Preflight Check" failed with a `ValueError` due to differences in how the `/auth` and `/check` endpoints handled incoming JSON payloads.
- **Solution:** Refactored the `load` method in `handler.py` to handle both nested and unwrapped credential payloads, ensuring compatibility with both endpoints.
- **Outcome:** Preflight checks and authentication now work seamlessly, regardless of payload structure.
