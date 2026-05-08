#!/bin/bash
# Setup script for audit_pyside

set -e # Exit on error

echo "Starting Jules environment setup..."

# Install dependencies using uv
# Jules pre-installs uv 0.7.13+
if command -v uv &> /dev/null; then
    echo "Using uv to sync dependencies..."
    uv sync
else
    echo "uv not found, using pip..."
    pip install -r requirements.txt
fi

# Verify critical dependencies
echo "Verifying environment..."
python --version
uv run ruff --version || true

# Run basic check to ensure environment is sane
echo "Environment check complete."
