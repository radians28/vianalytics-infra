#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status
set -e

# Configuration for Airflow 3.3.2 constraints
AIRFLOW_VERSION="3.3.2"
PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

echo "===================================================="
echo "🚀 Initializing Airflow 3.3.2 Submodule Installer"
echo "🐍 Python Version detected: $PYTHON_VERSION"
echo "===================================================="

# Determine the best package installer tool available (Airflow 3 explicitly supports pip and uv)
if command -v uv &> /dev/null; then
    INSTALL_CMD="uv pip install"
    echo "✨ Using 'uv' for ultra-fast dependency resolution."
else
    INSTALL_CMD="pip install"
    echo "📦 'uv' not found. Falling back to standard 'pip'."
fi

# Step 1: Ensure core Airflow Task SDK and main environment alignment exist
echo "🔗 Aligning base layer with constraints..."
$INSTALL_CMD "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Step 2: Recursively search and install all requirements from submodules
echo "🔍 Searching for requirements.txt files within submodules..."

# Find all requirements.txt files ignoring hidden directories (like .git or virtualenvs)
while IFS= read -r req_file; do
    echo "----------------------------------------------------"
    echo "📂 Found submodule requirements: $req_file"
    echo "📥 Installing dependencies matching constraints..."
    
    # Run the installation bound to official Airflow version constraints to prevent dependency hell
    $INSTALL_CMD -r "$req_file" --constraint "${CONSTRAINT_URL}"
    
done < <(find . -type d \( -name ".git" -o -name "venv" -o -name ".venv" \) -prune -o -name "requirements.txt" -print)

# Step 3: Validate environment consistency
echo "----------------------------------------------------"
echo "🔬 Running environment integrity check..."
if pip check; then
    echo "✅ Success! All submodule dependencies resolved without conflicts."
else
    echo "⚠️ Warning: Dependency conflicts detected. Check logs above."
    exit 1
fi
