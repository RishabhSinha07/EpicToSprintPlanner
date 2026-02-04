# Local Testing Instructions

To run the local E2E workflow, use the `test_workflow_local.py` script. This script bypasses S3 and runs the entire processing pipeline on your local machine.

### Prerequisites

You must set your AWS credentials in the environment before running the script:

```bash
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
export AWS_REGION="us-east-1"  # Or your preferred region
```

### Running the Test

Run the script and point it to a document (PDF, DOCX, MD, or TXT):

```bash
python3 local_tests_run/test_workflow_local.py sample-documents/sample-epic.md
```

### Viewing Results

The results will be saved in the `_temp_output/` directory by default:
- `stories.json`: Detailed user stories with metadata
- `jira_import.json`: Stories formatted for Jira import
- `summary.txt`: A human-readable summary of all generated stories
