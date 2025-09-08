#!/bin/bash
set -euo pipefail

# ==============================================================================
# Enhanced Markdown to Branded PDF Conversion
# 
# Creates a professional, visually appealing PDF with:
# - Conflixis branding and colors
# - Better typography and spacing
# - Enhanced section formatting
# - Professional headers and footers
# ==============================================================================

# --- Input Validation ---
if [[ $# -eq 0 ]]; then
    echo "Usage: $0 /path/to/report.md"
    exit 1
fi

MD_FILE="$1"

if [[ ! -f "$MD_FILE" ]]; then
    echo "Error: File not found: '$MD_FILE'"
    exit 1
fi

# --- Configuration ---
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PDF_FILE="${MD_FILE%.md}_enhanced.pdf"

# --- Pre-flight Checks ---
if ! command -v pandoc &> /dev/null; then
    echo "Error: pandoc is not installed. Please install it first."
    echo "Ubuntu/Debian: sudo apt-get install pandoc"
    echo "See: https://pandoc.org/installing.html"
    exit 1
fi

if ! command -v xelatex &> /dev/null; then
    echo "Error: xelatex is not installed. Please install texlive-xetex."
    echo "Ubuntu/Debian: sudo apt-get install texlive-xetex texlive-fonts-extra"
    exit 1
fi

# --- Pre-process Markdown ---
# Create a temporary file with enhanced formatting
TEMP_MD="${MD_FILE%.md}_temp.md"
cp "$MD_FILE" "$TEMP_MD"

# Add metadata if not present
if ! grep -q "^---$" "$TEMP_MD"; then
    # Extract title from first # heading
    TITLE=$(grep "^# " "$TEMP_MD" | head -1 | sed 's/^# //')
    DATE=$(date "+%B %d, %Y")
    
    # Prepend YAML front matter
    cat > "${TEMP_MD}.new" << EOF
---
title: "$TITLE"
subtitle: "Comprehensive Financial Relationships Analysis"
author: "Conflixis Data Analytics Division"
date: "$DATE"
---

EOF
    cat "$TEMP_MD" >> "${TEMP_MD}.new"
    mv "${TEMP_MD}.new" "$TEMP_MD"
fi

# --- Conversion ---
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🎨 Enhanced PDF Conversion for Healthcare COI Report"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📄 Source: $(basename "$MD_FILE")"
echo "📊 Output: $(basename "$PDF_FILE")"
echo ""
echo "[1/3] Pre-processing markdown..."
echo "[2/3] Converting to PDF with enhanced styling..."

pandoc "$TEMP_MD" \
  --defaults "$SCRIPT_DIR/pandoc-pdf-enhanced.yaml" \
  --resource-path "$SCRIPT_DIR:$(dirname "$MD_FILE")" \
  -o "$PDF_FILE" 2>&1 | grep -v "Missing character" || true

# Clean up temp file
rm -f "$TEMP_MD"

echo "[3/3] Post-processing complete"
echo ""

if [[ -f "$PDF_FILE" ]]; then
    FILE_SIZE=$(du -h "$PDF_FILE" | cut -f1)
    echo "✅ Success! PDF created: $PDF_FILE ($FILE_SIZE)"
    echo ""
    echo "📋 Features included:"
    echo "   • Conflixis branding and colors"
    echo "   • Professional headers and footers"
    echo "   • Enhanced typography and spacing"
    echo "   • Styled section headings"
    echo "   • Table of contents with hyperlinks"
    echo ""
    echo "💡 Tip: Open with your PDF viewer to see the enhanced formatting"
else
    echo "❌ Error: PDF creation failed"
    exit 1
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"