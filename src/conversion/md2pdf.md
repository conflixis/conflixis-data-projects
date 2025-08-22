# Markdown to PDF Conversion Guide

## Quick Start

Convert any markdown file to PDF using pandoc:

```bash
pandoc input.md -o output.pdf
```

## Installation

### Ubuntu/Debian
```bash
sudo apt update
sudo apt install -y pandoc texlive-xetex
```

### macOS
```bash
brew install pandoc
brew install --cask mactex  # Or basictex for smaller installation
```

### Windows
Download and install from: https://pandoc.org/installing.html

## Common Usage Examples

### Basic Conversion
```bash
pandoc document.md -o document.pdf
```

### With Custom Margins (Recommended)
```bash
# Narrow margins (0.5 inch all sides)
pandoc document.md -o document.pdf -V geometry:margin=0.5in

# Different margins for each side
pandoc document.md -o document.pdf -V geometry:left=0.5in,right=0.5in,top=0.75in,bottom=0.75in

# Very tight margins (0.25 inch)
pandoc document.md -o document.pdf -V geometry:margin=0.25in
```

### With Table of Contents
```bash
pandoc document.md -o document.pdf --toc --toc-depth=3 -V geometry:margin=0.5in
```

### With Custom Styling
```bash
# With syntax highlighting for code blocks
pandoc document.md -o document.pdf --highlight-style=tango

# With custom font size
pandoc document.md -o document.pdf -V fontsize=11pt

# With line numbers for code blocks
pandoc document.md -o document.pdf --listings
```

### Professional Document Settings
```bash
pandoc document.md -o document.pdf \
  -V geometry:margin=0.75in \
  -V fontsize=11pt \
  -V linkcolor=blue \
  -V urlcolor=blue \
  --toc \
  --toc-depth=2 \
  --highlight-style=tango
```

## Page Setup Options

### Paper Size
```bash
# Letter (US Default)
pandoc document.md -o document.pdf -V papersize=letter

# A4 (International)
pandoc document.md -o document.pdf -V papersize=a4

# Legal
pandoc document.md -o document.pdf -V papersize=legal
```

### Orientation
```bash
# Landscape
pandoc document.md -o document.pdf -V geometry:landscape

# Portrait (default)
pandoc document.md -o document.pdf -V geometry:portrait
```

## Headers and Footers

### Add Page Numbers
```bash
pandoc document.md -o document.pdf \
  -V pagestyle=plain \
  --template=default
```

### Custom Headers/Footers (requires custom template)
```bash
pandoc document.md -o document.pdf \
  --template=custom-template.tex \
  -V header-left="Document Title" \
  -V footer-center="\\thepage"
```

## Batch Conversion

### Convert All Markdown Files in Directory
```bash
# Basic batch conversion
for file in *.md; do
  pandoc "$file" -o "${file%.md}.pdf" -V geometry:margin=0.5in
done

# With progress indicator
for file in *.md; do
  echo "Converting $file..."
  pandoc "$file" -o "${file%.md}.pdf" -V geometry:margin=0.5in
done
echo "All files converted!"
```

## PDF Engines

Pandoc supports multiple PDF engines:

```bash
# XeLaTeX (recommended for Unicode support)
pandoc document.md -o document.pdf --pdf-engine=xelatex

# pdfLaTeX (default)
pandoc document.md -o document.pdf --pdf-engine=pdflatex

# LuaLaTeX
pandoc document.md -o document.pdf --pdf-engine=lualatex

# ConTeXt
pandoc document.md -o document.pdf --pdf-engine=context

# HTML to PDF via wkhtmltopdf
pandoc document.md -o document.pdf --pdf-engine=wkhtmltopdf

# HTML to PDF via weasyprint
pandoc document.md -o document.pdf --pdf-engine=weasyprint
```

## Troubleshooting

### Missing LaTeX Package Errors
If you get errors about missing packages, install the full texlive:
```bash
sudo apt install texlive-full  # Large download, ~5GB
```

### Unicode/Font Issues
Use XeLaTeX engine for better Unicode support:
```bash
pandoc document.md -o document.pdf --pdf-engine=xelatex
```

### Tables Not Rendering Properly
Use pipe tables in markdown for best results:
```markdown
| Column 1 | Column 2 | Column 3 |
|----------|----------|----------|
| Data 1   | Data 2   | Data 3   |
```

### Images Not Showing
Ensure image paths are relative to the markdown file:
```markdown
![Alt text](./images/image.png)
```

## Advanced Features

### Include External Files
```bash
# Include YAML metadata
pandoc document.md metadata.yaml -o document.pdf

# Include multiple markdown files
pandoc chapter1.md chapter2.md chapter3.md -o book.pdf
```

### Custom CSS for HTML-based PDF
```bash
pandoc document.md -o document.pdf \
  --css=custom.css \
  --pdf-engine=weasyprint
```

### Variables and Metadata
Add to top of markdown file:
```yaml
---
title: "Document Title"
author: "Author Name"
date: "2024-08-20"
geometry: margin=0.5in
fontsize: 11pt
---
```

## Examples from This Project

### Research Paper Conversion
```bash
# Convert the RX-OP Enhanced deep dive analysis
cd /home/incent/conflixis-data-projects/projects/012-rx-op-enhanced/research
pandoc deep_dive_analysis.md -o deep_dive_analysis.pdf -V geometry:margin=0.5in

# With table of contents
pandoc deep_dive_analysis.md -o deep_dive_analysis.pdf \
  -V geometry:margin=0.5in \
  --toc \
  --toc-depth=3
```

### Executive Summary Conversion
```bash
cd /home/incent/conflixis-data-projects/projects/012-rx-op-enhanced
pandoc executive_summary.md -o executive_summary.pdf \
  -V geometry:margin=0.75in \
  -V fontsize=11pt \
  -V linkcolor=blue
```

## Tips and Best Practices

1. **Always preview** your PDF after conversion to check formatting
2. **Use narrow margins** (0.5-0.75in) to maximize content per page
3. **Include TOC** for documents longer than 5 pages
4. **Use XeLaTeX** engine for documents with special characters
5. **Test margins** - what looks good on screen may need adjustment for print
6. **Keep it simple** - start with basic conversion, add options as needed

## Quick Reference Card

```bash
# Most common command you'll use:
pandoc file.md -o file.pdf -V geometry:margin=0.5in

# For professional documents:
pandoc file.md -o file.pdf -V geometry:margin=0.75in --toc -V fontsize=11pt

# For maximum content density:
pandoc file.md -o file.pdf -V geometry:margin=0.25in -V fontsize=10pt
```

---

*Last updated: 2024-08-20*  
*Location: `/home/incent/conflixis-data-projects/src/conversion/md2pdf.md`*