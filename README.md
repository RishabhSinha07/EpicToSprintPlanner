# Epic to Sprint Planner

A serverless document processing pipeline that transforms large documents into INVEST-compliant user stories with acceptance criteria, story point estimations, and dependency mapping.

## Features

- Process documents up to 1000+ pages through intelligent chunking
- Support multiple input formats: PDF, Word (.docx), Markdown, plain text
- Generate INVEST-compliant user stories with acceptance criteria
- Provide story point estimations and dependency mapping
- Export to multiple formats including Jira-compatible JSON
- Fully serverless, auto-scaling architecture using AWS Lambda and Bedrock

## Architecture

```
Input Document (S3)
  → Chunker Lambda (splits by sections)
  → Story Generator Lambda (calls Bedrock)
  → Aggregator Lambda (merges & deduplicates)
  → Output Stories (S3)
```

## Prerequisites

- Python 3.12+
- AWS CLI configured with appropriate credentials
- AWS SAM CLI installed
- Access to AWS Bedrock (Claude 3.5 Sonnet model)

## Project Structure

```
EpicToSprintPlanner/
├── src/
│   ├── lambdas/
│   │   ├── chunker/          # Document chunking Lambda
│   │   ├── story_generator/  # Story generation with Bedrock
│   │   └── aggregator/       # Story aggregation & deduplication
│   └── common/               # Shared utilities
├── tests/                    # Unit and integration tests
├── events/                   # Sample event payloads for local testing
├── template.yaml            # SAM template
└── README.md

## Local Development

### Setup

1. Install dependencies:
```bash
pip install -r requirements-dev.txt
```

2. Install dependencies for each Lambda:
```bash
cd src/lambdas/chunker && pip install -r requirements.txt -t .
cd ../story_generator && pip install -r requirements.txt -t .
cd ../aggregator && pip install -r requirements.txt -t .
```

### Local Testing

Test individual Lambdas locally using SAM:

```bash
# Test Chunker
sam local invoke ChunkerFunction -e events/chunker-event.json

# Test Story Generator
sam local invoke StoryGeneratorFunction -e events/story-generator-event.json

# Test Aggregator
sam local invoke AggregatorFunction -e events/aggregator-event.json
```

### Local E2E Workflow Testing

For a full end-to-end test without S3 dependencies, see the [local_tests_run](local_tests_run/README.md) directory:

```bash
# Set your Bedrock credentials
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"

# Run the local E2E script
python3 local_tests_run/test_workflow_local.py sample-documents/sample-epic.md
```

### Local API Testing

Start a local API Gateway:

```bash
sam local start-api
```

## Deployment

### First-time Deployment

1. Build the application:
```bash
sam build
```

2. Deploy with guided setup:
```bash
sam deploy --guided
```

Follow the prompts to configure:
- Stack name (e.g., epic-to-sprint-planner)
- AWS Region
- Confirm IAM role creation

### Subsequent Deployments

```bash
sam build && sam deploy
```

## Usage

### Upload a Document

After deployment, upload a document to the input bucket:

```bash
aws s3 cp your-document.pdf s3://epic-to-sprint-planner-input-{ACCOUNT_ID}/
```

The pipeline will automatically:
1. Chunk the document
2. Generate user stories for each chunk
3. Aggregate and deduplicate stories
4. Save results to the output bucket

### Retrieve Results

Download the processed stories:

```bash
aws s3 cp s3://epic-to-sprint-planner-output-{ACCOUNT_ID}/stories.json ./
```

## Development Phases

### Phase 1: Initial Setup (Current)
- [x] Project structure
- [ ] S3 buckets and IAM roles
- [ ] Chunker Lambda
- [ ] Story Generator Lambda
- [ ] Manual testing

### Phase 2: Orchestration
- [ ] Step Functions state machine
- [ ] Aggregator Lambda
- [ ] S3 trigger integration
- [ ] End-to-end testing

## Configuration

Environment variables can be configured in `template.yaml`:

- `CHUNK_SIZE`: Maximum tokens per chunk (default: 4000)
- `OVERLAP_SIZE`: Overlap between chunks (default: 200)
- `BEDROCK_MODEL_ID`: Bedrock model to use (default: Claude 3.5 Sonnet)

## Testing

Run unit tests:

```bash
pytest tests/
```

Run integration tests:

```bash
pytest tests/integration/
```

## License

MIT
