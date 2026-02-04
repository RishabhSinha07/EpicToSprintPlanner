#!/bin/bash

echo "============================================"
echo "Epic to Sprint Planner - Setup Script"
echo "============================================"
echo ""

# Check prerequisites
echo "Checking prerequisites..."

# Check Python version
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
    echo "✓ Python 3 found: $PYTHON_VERSION"
else
    echo "✗ Python 3 not found. Please install Python 3.12 or higher."
    exit 1
fi

# Check AWS CLI
if command -v aws &> /dev/null; then
    AWS_VERSION=$(aws --version 2>&1 | cut -d' ' -f1)
    echo "✓ AWS CLI found: $AWS_VERSION"
else
    echo "⚠ AWS CLI not found. You'll need it for deployment."
    echo "  Install from: https://aws.amazon.com/cli/"
fi

# Check SAM CLI
if command -v sam &> /dev/null; then
    SAM_VERSION=$(sam --version | cut -d' ' -f4)
    echo "✓ AWS SAM CLI found: $SAM_VERSION"
else
    echo "⚠ AWS SAM CLI not found. You'll need it for deployment."
    echo "  Install from: https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html"
fi

echo ""
echo "Installing Python dependencies..."

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install development dependencies
echo "Installing development dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements-dev.txt

echo ""
echo "✓ Development dependencies installed"

# Install Lambda dependencies
echo ""
echo "Installing Lambda dependencies..."
echo "  - Chunker Lambda..."
cd src/lambdas/chunker && pip install -q -r requirements.txt -t . && cd ../../..

echo "  - Story Generator Lambda..."
cd src/lambdas/story_generator && pip install -q -r requirements.txt -t . && cd ../../..

echo "  - Aggregator Lambda..."
cd src/lambdas/aggregator && pip install -q -r requirements.txt -t . && cd ../../..

echo "✓ Lambda dependencies installed"

echo ""
echo "============================================"
echo "Setup Complete!"
echo "============================================"
echo ""
echo "Next steps:"
echo ""
echo "1. Activate the virtual environment:"
echo "   source venv/bin/activate"
echo ""
echo "2. Run local tests:"
echo "   python3 local_test.py process"
echo ""
echo "3. Run unit tests:"
echo "   pytest tests/"
echo ""
echo "4. Deploy to AWS (requires AWS credentials):"
echo "   sam build && sam deploy --guided"
echo ""
echo "For more information, see QUICKSTART.md"
echo ""
