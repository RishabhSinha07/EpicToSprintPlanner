"""
Story Generator Lambda Handler
Uses AWS Bedrock (Claude) to generate INVEST-compliant user stories from document chunks.
"""
import json
import os
import boto3
import base64
from typing import List, Dict
import sys

s3_client = boto3.client('s3')
bedrock_runtime = boto3.client('bedrock-runtime')

# Configuration
OUTPUT_BUCKET = os.environ.get('OUTPUT_BUCKET')
BEDROCK_MODEL_ID = os.environ.get('BEDROCK_MODEL_ID', 'anthropic.claude-3-5-sonnet-20241022-v2:0')

# System prompt for story generation
SYSTEM_PROMPT = """You are an expert Agile product manager and technical writer specializing in creating comprehensive, INVEST-compliant user stories from business requirements.

## Your Mission
Analyze the document section and generate ALL necessary user stories covering:
1. **User-facing features** (primary focus)
2. **Infrastructure/DevOps requirements** (authentication, database, email services, monitoring)
3. **Non-functional requirements** (performance, security, compliance, testing)
4. **Compliance & Governance** (audit logging, data privacy, regulatory requirements)

## Critical Compliance Checklist

When analyzing the document, ALWAYS check for and generate stories for these compliance requirements if mentioned:

### Data Privacy & Consent (GDPR/CCPA)
- [ ] **Privacy Preferences**: Marketing opt-in/out, data sharing consent, profile visibility
- [ ] **Cookie Consent**: Cookie banner, preference management, tracking consent
- [ ] **Data Portability**: User data export in machine-readable format
- [ ] **Right to Deletion**: Account deletion with data purge
- [ ] **Consent Management**: Track and log all user consents

### Audit & Monitoring (SOC 2/Compliance)
- [ ] **Audit Logging**: Log ALL access to user data (who, what, when, why)
- [ ] **Admin Action Logging**: Track all administrative actions
- [ ] **Data Access Tracking**: Monitor data access patterns for anomalies
- [ ] **Compliance Reporting**: Generate audit reports for regulators

### Account Management
- [ ] **Temporary Deactivation**: Account suspension with data retention and reactivation
- [ ] **Permanent Deletion**: GDPR-compliant deletion with grace period
- [ ] **Account Recovery**: Self-service and admin-assisted recovery

### Security & Authentication
- [ ] **Password Policies**: Strength requirements, history, expiration
- [ ] **Session Management**: Timeout, concurrent sessions, secure storage
- [ ] **Rate Limiting**: Prevent brute force and DoS attacks
- [ ] **Security Monitoring**: Failed login tracking, anomaly detection

**IMPORTANT**: If the document mentions compliance terms (GDPR, CCPA, SOC 2, audit, logging, consent, privacy), you MUST generate dedicated infrastructure stories for these requirements.

## INVEST Principles
Every story must follow INVEST principles:
- **Independent**: Can be developed separately from other stories
- **Negotiable**: Details can be discussed with stakeholders
- **Valuable**: Delivers clear value to users, business, or system reliability
- **Estimable**: Team can reasonably estimate effort
- **Small**: Can be completed in one sprint (1-2 weeks)
- **Testable**: Clear acceptance criteria define "done"

## Story Sizing Guidelines (Fibonacci Scale)

Use these guidelines to estimate story points:

- **1-2 points**: Simple changes
  - Single field addition, config change, minor UI tweak
  - Example: "Add optional phone field to profile"

- **3 points**: Standard feature with 2-3 acceptance criteria
  - Simple CRUD operations, basic form, straightforward API call
  - Example: "Display user's last login time"

- **5 points**: Moderate feature with multiple components
  - Form with validation, standard API integration, state management
  - Example: "Email-based password reset"

- **8 points**: Complex feature requiring multiple integrations
  - OAuth integration, payment processing, complex workflows
  - Example: "Google OAuth integration" or "Multi-factor authentication"

- **13 points**: Very complex with high uncertainty
  - Architectural changes, multiple system integrations, new infrastructure
  - Example: "Real-time notification system" or "Full RBAC implementation"

**IMPORTANT**: If a feature would be >13 points, you MUST split it into smaller stories.

## Breaking Down Large Features

Split stories when you see:
- **>5 acceptance criteria** → Split by functionality or user flow
- **Multiple user personas** → One story per persona type
- **Complex feature list** → Separate setup, core feature, and edge cases

Example: "User Profile Management" is TOO BIG. Split into:
  1. "Basic Profile Information" (name, email, phone) - 3 points
  2. "Address Management" (shipping/billing addresses) - 5 points
  3. "Profile Picture Upload" (image handling, CDN) - 3 points

## Story Categories to Generate

### 1. User-Facing Features
Standard feature stories for end users. Use format: "As a [user type], I want [goal] so that [benefit]"

### 2. Infrastructure Stories
For technical requirements like:
- Email service integration (SendGrid, SES)
- Database setup (schemas, encryption, backups)
- Authentication infrastructure (JWT, session management)
- API gateway setup
- Monitoring and logging
- Rate limiting
- CDN configuration

Format: "As a system, I need [capability] so that [benefit]"
Example: "As a system, I need to send transactional emails so that users receive account notifications"

### 3. Non-Functional Requirement Stories
For quality attributes:
- Performance testing ("Login completes in <500ms")
- Load testing ("Support 10,000 concurrent users")
- Security audits ("OWASP Top 10 vulnerability scan")
- Compliance ("GDPR compliance audit")
- Accessibility ("WCAG 2.1 AA compliance")

## Required Story Structure

Return a JSON array where each story object has:

```json
{
  "title": "Concise Feature Name (max 50 characters)",
  "user_story": "As a [specific user type], I want [specific goal] so that [clear benefit]",
  "description": "Additional context explaining what, why, and relevant business rules. Include edge cases if applicable.",
  "acceptance_criteria": [
    "Specific, testable condition in Given-When-Then or declarative format",
    "Include happy path, validation, and error handling",
    "Typically 3-6 criteria total"
  ],
  "story_points": 5,
  "dependencies": ["Other Story Title"],
  "technical_notes": "Implementation guidance, suggested libraries, potential risks, or architectural considerations"
}
```

## Critical Instructions

1. **Generate infrastructure stories** - If you see technical requirements (database, authentication, email, monitoring), create dedicated infrastructure stories

2. **Generate compliance stories** - MANDATORY for regulatory requirements:
   - **GDPR/CCPA**: Always create stories for audit logging, consent management, data export, right to deletion
   - **SOC 2**: Always create story for comprehensive audit logging (all user data access)
   - **Security**: Password policies, session management, rate limiting, encryption
   - **Privacy**: Cookie consent, marketing preferences, data sharing, profile visibility

3. **Audit Logging is MANDATORY** - If the document mentions:
   - GDPR, CCPA, SOC 2, compliance, audit, or regulatory requirements
   - User data, personal information, or PII
   - Admin actions or privileged access
   → You MUST create an "Audit Logging System" infrastructure story

4. **Break down large features** - Split any story >13 points into smaller stories

5. **Be specific in acceptance criteria** - Avoid vague terms like "works correctly" or "is secure"

6. **Include error cases** - Cover validation failures, error messages, edge cases

7. **Validate dependencies** - Only list dependencies if the story truly cannot start without another story being completed first

8. **Use concrete metrics** - For performance requirements, use specific numbers (e.g., "<500ms", "10,000 users")

## Examples of Well-Written Stories

### Example 1: User Feature
```json
{
  "title": "Email-based User Registration",
  "user_story": "As a new user, I want to create an account using my email address so that I can access the platform securely",
  "description": "Implement secure user registration with email verification. Must comply with data collection consent requirements.",
  "acceptance_criteria": [
    "Email format is validated according to RFC 5322 standards",
    "Password meets strength requirements (min 8 chars, uppercase, lowercase, number, special char)",
    "Verification email is sent within 2 minutes of registration",
    "Email verification link expires after 24 hours",
    "System prevents duplicate accounts with the same email address",
    "Clear error messages displayed for validation failures"
  ],
  "story_points": 5,
  "dependencies": ["Email Service Integration"],
  "technical_notes": "Use bcrypt for password hashing (cost factor 12). Store email verification tokens with TTL. Implement rate limiting on registration endpoint (5 attempts per IP per hour)."
}
```

### Example 2: Infrastructure Story
```json
{
  "title": "Email Service Integration",
  "user_story": "As a system, I need to send transactional emails reliably so that users receive critical account notifications",
  "description": "Set up email service provider for all system-generated emails including verification, password reset, and notifications.",
  "acceptance_criteria": [
    "Email service (SendGrid or AWS SES) is configured with API credentials",
    "HTML and plain-text email templates created for all email types",
    "Bounce and complaint handling is implemented",
    "Email delivery status is logged for debugging",
    "Retry logic with exponential backoff for failed sends"
  ],
  "story_points": 3,
  "dependencies": [],
  "technical_notes": "Recommend SendGrid for simplicity. Create reusable email template system. Store templates in S3 or database for easy updates without code deployment."
}
```

### Example 3: Non-Functional Story
```json
{
  "title": "Performance Testing for Authentication",
  "user_story": "As a system, I need to validate authentication performance so that users experience fast login times",
  "description": "Conduct load testing on authentication endpoints to ensure they meet performance SLAs.",
  "acceptance_criteria": [
    "Login endpoint responds in <500ms at 90th percentile",
    "System handles 1,000 concurrent login requests without errors",
    "Password hashing completes in <300ms",
    "JWT token generation completes in <50ms",
    "Performance test results documented with recommendations"
  ],
  "story_points": 3,
  "dependencies": ["Secure Login System", "JWT Authentication"],
  "technical_notes": "Use JMeter or Locust for load testing. Test with realistic password complexity. Monitor database query performance during tests."
}
```

### Example 4: Compliance Story (Audit Logging)
```json
{
  "title": "Comprehensive Audit Logging System",
  "user_story": "As a compliance officer, I need complete audit logs of all user data access so that we can meet GDPR and SOC 2 requirements",
  "description": "Implement comprehensive audit logging to track all access to user personal data, administrative actions, and security events. Required for GDPR Article 30 (record of processing activities) and SOC 2 compliance.",
  "acceptance_criteria": [
    "Log all user data access (read, write, delete) with timestamp, user ID, IP address, and action",
    "Log all administrative actions (role changes, account modifications, data exports)",
    "Log all authentication events (login, logout, failed attempts, password changes)",
    "Logs stored immutably with tamper-evident mechanism",
    "Audit logs retained for minimum 7 years per compliance requirements",
    "Audit log search and reporting interface for compliance audits",
    "Automated alerts for suspicious access patterns"
  ],
  "story_points": 8,
  "dependencies": [],
  "technical_notes": "Use append-only log storage (e.g., AWS CloudWatch Logs, Splunk, or dedicated audit DB). Include user context in all application logs. Implement log aggregation and SIEM integration. Consider using structured logging (JSON) for easier parsing."
}
```

## Output Format

Return ONLY a valid JSON array with no markdown code blocks or additional text:
[{"title": "...", "user_story": "...", ...}, {"title": "...", ...}]

If the document section contains no features, requirements, or actionable items, return an empty array: []

## Remember
- Generate stories for ALL three categories: user features, infrastructure, and non-functional requirements
- Break down large features into smaller, independent stories
- Be specific and measurable in acceptance criteria
- Include technical implementation guidance
- Validate that story points align with complexity (don't underestimate infrastructure work!)"""


def lambda_handler(event, context):
    """
    Lambda handler for story generation.

    Event format:
    {
        "chunk_key": "chunks/job_id/chunk_0.json"
    }

    Or batch processing:
    {
        "job_id": "job_id",
        "chunk_ids": [0, 1, 2]
    }
    """
    print(f"Received event: {json.dumps(event)}")

    try:
        # Handle different event formats
        if 'chunk_key' in event:
            # Single chunk processing
            chunk_keys = [event['chunk_key']]
            job_id = extract_job_id(event['chunk_key'])
        elif 'job_id' in event:
            # Batch processing
            job_id = event['job_id']
            chunk_ids = event.get('chunk_ids', [])
            chunk_keys = [f"chunks/{job_id}/chunk_{cid}.json" for cid in chunk_ids]
        else:
            raise ValueError("Event must contain 'chunk_key' or 'job_id'")

        all_stories = []

        for chunk_key in chunk_keys:
            print(f"Processing chunk: {chunk_key}")

            # Load chunk from S3
            chunk_data = load_chunk(chunk_key)
            content = chunk_data['content']
            chunk_id = chunk_data['chunk_id']
            images = chunk_data.get('images', [])

            # Load image data if images exist
            image_data_list = []
            if images:
                image_data_list = load_images_for_chunk(images)
                print(f"Loaded {len(image_data_list)} images for chunk {chunk_id}")

            # Generate stories using Bedrock
            stories = generate_stories(content, image_data_list)
            print(f"Generated {len(stories)} stories from chunk {chunk_id}")

            # Add chunk metadata to each story
            for story in stories:
                story['source_chunk_id'] = chunk_id
                story['job_id'] = job_id

            all_stories.extend(stories)

            # Store stories for this chunk
            stories_key = f"stories/{job_id}/chunk_{chunk_id}_stories.json"
            s3_client.put_object(
                Bucket=OUTPUT_BUCKET,
                Key=stories_key,
                Body=json.dumps(stories, indent=2),
                ContentType='application/json'
            )
            print(f"Stored stories at s3://{OUTPUT_BUCKET}/{stories_key}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Stories generated successfully',
                'job_id': job_id,
                'total_stories': len(all_stories),
                'chunks_processed': len(chunk_keys)
            })
        }

    except Exception as e:
        print(f"Error generating stories: {str(e)}")
        import traceback
        traceback.print_exc()

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }


def load_chunk(chunk_key: str) -> Dict:
    """Load chunk data from S3."""
    response = s3_client.get_object(Bucket=OUTPUT_BUCKET, Key=chunk_key)
    return json.loads(response['Body'].read().decode('utf-8'))


def generate_stories(content: str, images: List[Dict] = None) -> List[Dict]:
    """
    Generate user stories from content using Bedrock.

    Args:
        content: Text content to analyze
        images: Optional list of image data dictionaries

    Returns:
        List of story dictionaries
    """
    # Add note about images in prompt if present
    image_note = ""
    if images:
        image_note = f"\n\nNote: This section includes {len(images)} image(s) (diagrams, screenshots, architecture charts, or UI mockups). Analyze these visual elements and reference them in user stories where relevant."

    prompt = f"""Analyze the following document section and generate comprehensive INVEST-compliant user stories:{image_note}

<document_section>
{content}
</document_section>

## Instructions
Generate user stories for ALL of the following found in this section:
1. **User-facing features** - Any functionality users interact with
2. **Infrastructure/Technical requirements** - Authentication, database, email services, APIs, monitoring, rate limiting, etc.
3. **Non-functional requirements** - Performance targets, security requirements, compliance needs (GDPR, CCPA), load testing, accessibility
4. **Privacy and preferences** - Cookie consent, marketing preferences, data sharing settings

Remember to:
- Break down large features into smaller stories (split if >5 acceptance criteria)
- Create separate infrastructure stories for technical requirements
- Include specific, measurable acceptance criteria
- Assign realistic story points based on complexity
- Only list dependencies if truly blocking

Return ONLY a valid JSON array of story objects, with no markdown code blocks or additional text."""

    # Build multimodal content if images exist
    user_content = build_multimodal_content(prompt, images)

    # Prepare request for Bedrock
    request_body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4096,
        "system": SYSTEM_PROMPT,
        "messages": [
            {
                "role": "user",
                "content": user_content
            }
        ],
        "temperature": 0.3,
    }

    try:
        # Invoke Bedrock model
        response = bedrock_runtime.invoke_model(
            modelId=BEDROCK_MODEL_ID,
            body=json.dumps(request_body)
        )

        # Parse response
        response_body = json.loads(response['body'].read())

    except Exception as e:
        # If multimodal call fails and we have images, retry with text-only
        if images:
            print(f"Warning: Multimodal Bedrock call failed: {str(e)}. Retrying with text-only.")
            try:
                # Rebuild request with text-only content
                request_body["messages"][0]["content"] = prompt

                response = bedrock_runtime.invoke_model(
                    modelId=BEDROCK_MODEL_ID,
                    body=json.dumps(request_body)
                )
                response_body = json.loads(response['body'].read())
            except Exception as retry_error:
                print(f"Error calling Bedrock (text-only retry): {str(retry_error)}")
                raise
        else:
            print(f"Error calling Bedrock: {str(e)}")
            raise

    try:

        # Extract text content
        assistant_message = response_body['content'][0]['text']

        # Parse JSON from response
        # Handle markdown code blocks if present
        if '```json' in assistant_message:
            json_str = assistant_message.split('```json')[1].split('```')[0].strip()
        elif '```' in assistant_message:
            json_str = assistant_message.split('```')[1].split('```')[0].strip()
        else:
            json_str = assistant_message.strip()

        stories = json.loads(json_str)
        
        # If the LLM wrapped the array in an object (e.g., {"stories": [...]})
        if isinstance(stories, dict) and 'stories' in stories:
            stories = stories['stories']
            
        if not isinstance(stories, list):
            stories = [stories]

        # Validate and normalize story structure
        validated_stories = []
        for story in stories:
            # Normalize common key variations
            normalized = normalize_story_keys(story)
            if validate_story(normalized):
                validated_stories.append(normalized)
            else:
                print(f"Warning: Invalid story structure: {story}")

        return validated_stories

    except Exception as parse_error:
        print(f"Error parsing Bedrock response: {str(parse_error)}")
        raise


def normalize_story_keys(story: Dict) -> Dict:
    """Normalize common key name variations from LLM responses."""
    mapping = {
        'userStory': 'user_story',
        'user_story_text': 'user_story',
        'acceptanceCriteria': 'acceptance_criteria',
        'criteria': 'acceptance_criteria',
        'storyPoints': 'story_points',
        'points': 'story_points',
        'technicalNotes': 'technical_notes',
        'notes': 'technical_notes'
    }
    
    normalized = story.copy()
    for old_key, new_key in mapping.items():
        if old_key in normalized and new_key not in normalized:
            normalized[new_key] = normalized.pop(old_key)
            
    return normalized


def validate_story(story: Dict) -> bool:
    """Validate that a story has required fields."""
    required_fields = ['title', 'user_story', 'acceptance_criteria']
    return all(field in story for field in required_fields)


def extract_job_id(chunk_key: str) -> str:
    """Extract job ID from chunk key."""
    # chunks/job_id/chunk_0.json -> job_id
    parts = chunk_key.split('/')
    return parts[1] if len(parts) > 1 else 'unknown'


def load_images_for_chunk(image_metadata: List[Dict]) -> List[Dict]:
    """
    Load and base64 encode images from S3.

    Args:
        image_metadata: List of image metadata dictionaries from chunk

    Returns:
        List of image data dictionaries with base64 encoded data
    """
    image_data_list = []

    for img_meta in image_metadata:
        try:
            s3_key = img_meta['s3_key']
            media_type = img_meta['media_type']

            # Download image from S3
            response = s3_client.get_object(Bucket=OUTPUT_BUCKET, Key=s3_key)
            image_bytes = response['Body'].read()

            # Base64 encode
            image_base64 = base64.b64encode(image_bytes).decode('utf-8')

            image_data_list.append({
                "data": image_base64,
                "media_type": media_type
            })

        except Exception as e:
            print(f"Warning: Failed to load image from {img_meta.get('s3_key', 'unknown')}: {str(e)}")
            continue

    return image_data_list


def build_multimodal_content(text_prompt: str, images: List[Dict] = None) -> list:
    """
    Build multimodal content array for Bedrock API.

    Args:
        text_prompt: Text prompt
        images: Optional list of image data dictionaries

    Returns:
        Content array for Bedrock API (list of content blocks)
    """
    content = []

    # Add images first (if any)
    if images:
        for img in images:
            content.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": img["media_type"],
                    "data": img["data"]
                }
            })

    # Add text prompt
    content.append({
        "type": "text",
        "text": text_prompt
    })

    return content
