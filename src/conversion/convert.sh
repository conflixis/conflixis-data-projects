#!/bin/bash
set -euo pipefail

# ==============================================================================
# Markdown to Branded PDF Conversion Tool
#
# Description:
#   This script converts a given Markdown file to a professionally branded
#   PDF using a predefined Pandoc template.
#
# Usage:
#   ./convert.sh /path/to/your/document.md
#
# The script will generate the PDF in the same directory as the source file.
# ==============================================================================

# --- 1. Input Validation ---
if [[ $# -eq 0 ]]; then
    echo "Usage: $0 /path/to/your/document.md"
    exit 1
fi

MD_FILE="$1"

if [[ ! -f "$MD_FILE" ]]; then
    echo "Error: Source file not found at '$MD_FILE'"
    exit 1
fi

# --- 2. Configuration ---
# The directory where this script is located, used to find the template.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
TEMPLATE_DIR="$SCRIPT_DIR/template"

# Derive the output PDF path from the input Markdown file.
PDF_FILE="${MD_FILE%.md}.pdf"

# --- 3. Pre-flight Check ---
# Ensure Pandoc is installed before we proceed.
if ! command -v pandoc &> /dev/null; then
    echo "Error: pandoc is not installed. Please install it to continue."
    echo "See: https://pandoc.org/installing.html"
    exit 1
fi

# --- 4. Conversion ---
echo "[1/2] Starting conversion for: $(basename "$MD_FILE")"

# The --resource-path tells pandoc where to find template files like header.tex
# and the assets/logo.png. This is crucial for the paths inside header.tex to resolve correctly.
pandoc "$MD_FILE" \
  --defaults "$TEMPLATE_DIR/pandoc-pdf.yaml" \
  --resource-path "$TEMPLATE_DIR" \
  -o "$PDF_FILE"

echo "[2/2] Successfully created PDF: $PDF_FILE"
echo "Done."