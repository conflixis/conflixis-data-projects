# Markdown to Branded PDF Conversion Tool

## Overview

This tool provides a standardized way to convert Markdown files into professionally branded PDF documents. It uses `pandoc` and a predefined LaTeX template to ensure consistent branding, fonts, and colors across all generated reports.

## Prerequisites

Before using this tool, you must have the following software installed:

1.  **Pandoc**: The core document conversion engine.
    -   *Installation*: [https://pandoc.org/installing.html](https://pandoc.org/installing.html)

2.  **XeTeX**: A modern TeX engine with excellent support for Unicode and modern fonts.
    -   *On Ubuntu/Debian*: `sudo apt-get install texlive-xetex`
    -   *On macOS (via MacTeX)*: `brew install --cask mactex`

## How to Use

The conversion process is handled by a single script.

1.  **Navigate to the script directory**:
    ```bash
    cd /path/to/conflixis-data-projects/src/conversion
    ```

2.  **Run the script with the path to your Markdown file**:
    ```bash
    ./convert.sh /path/to/your/report.md
    ```

The script will automatically generate the PDF in the same directory as your source Markdown file. For example, running the command above will create `/path/to/your/report.pdf`.

## Template and Branding

All branding and styling are managed within the `template/` directory:

-   `template/pandoc-pdf.yaml`: The main configuration file. It controls fonts, margins, colors, and other styling options.
-   `template/header.tex`: A LaTeX file that defines the custom page headers and footers, including the company logo.
-   `template/assets/`: This directory contains all static assets, such as the company logo.

To update the branding, you can modify the files in the `template/` directory. These changes will be applied to all future conversions.
