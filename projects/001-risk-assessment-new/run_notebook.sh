#!/bin/bash
# Quick launcher for Risk Assessment notebook

echo "🚀 Launching Risk Assessment Notebook..."
echo "=================================="

# Activate poetry environment and start Jupyter
poetry run jupyter notebook projects/001-risk-assessment-new/Risk_assessment_new.ipynb

echo "✨ Notebook session ended"