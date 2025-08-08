#!/usr/bin/env python3
"""
Simple runner script for local development
"""
import os
import sys
import subprocess
import webbrowser
import time
from pathlib import Path


def check_requirements():
    """Check if required packages are installed"""
    try:
        import fastapi
        import uvicorn
        import pandas
        import pyarrow
        return True
    except ImportError:
        return False


def install_requirements():
    """Install required packages"""
    print("Installing requirements...")
    requirements_path = Path(__file__).parent / "requirements.txt"
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", str(requirements_path)])
    print("Requirements installed successfully!")


def main():
    """Main entry point"""
    print("=" * 60)
    print("COI Disclosure Review System - Local Runner")
    print("=" * 60)
    print()
    
    # Check if requirements are installed
    if not check_requirements():
        print("Some required packages are missing.")
        response = input("Would you like to install them now? (y/n): ")
        if response.lower() == 'y':
            install_requirements()
        else:
            print("Please install requirements manually:")
            print("  pip install -r requirements.txt")
            sys.exit(1)
    
    # Check if data exists
    data_dir = Path(__file__).parent.parent / "data" / "staging"
    if not data_dir.exists():
        print("Warning: Data directory not found.")
        print("The application will use sample data.")
        print()
    
    # Start the server
    print("Starting server...")
    print()
    print("The application will open in your browser shortly.")
    print("If it doesn't, navigate to: http://localhost:8000")
    print()
    print("Press Ctrl+C to stop the server")
    print("-" * 60)
    
    # Open browser after a short delay
    def open_browser():
        time.sleep(2)
        webbrowser.open("http://localhost:8000")
    
    import threading
    browser_thread = threading.Thread(target=open_browser)
    browser_thread.daemon = True
    browser_thread.start()
    
    # Run the server
    os.chdir(Path(__file__).parent)
    subprocess.call([sys.executable, "main.py"])


if __name__ == "__main__":
    main()