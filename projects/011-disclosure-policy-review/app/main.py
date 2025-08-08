"""
Main entry point for COI Disclosure Review System
Runs both API and serves frontend for local development
"""
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from api.main import create_api_app


def create_app() -> FastAPI:
    """Create the main application with API and frontend
    
    Returns:
        Configured FastAPI application
    """
    # Create main app
    app = FastAPI(
        title="COI Disclosure Review System",
        description="Complete system for managing COI disclosures",
        version="1.0.0"
    )
    
    # Create and mount API app
    api_app = create_api_app()
    app.mount("/api", api_app)
    
    # Mount frontend static files
    frontend_path = Path(__file__).parent / "frontend"
    app.mount("/frontend", StaticFiles(directory=str(frontend_path), html=True), name="frontend")
    
    # Create symlinks for data and config access
    data_path = Path(__file__).parent.parent / "data"
    config_path = Path(__file__).parent.parent / "config"
    
    # Serve data files (read-only)
    if data_path.exists():
        app.mount("/data", StaticFiles(directory=str(data_path)), name="data")
    
    # Root redirects to frontend
    @app.get("/")
    async def root():
        """Redirect to main viewer"""
        return FileResponse(str(frontend_path / "index.html"))
    
    @app.get("/dashboard")
    async def dashboard():
        """Serve dashboard"""
        return FileResponse(str(frontend_path / "dashboard.html"))
    
    return app


# Create app instance
app = create_app()


if __name__ == "__main__":
    import uvicorn
    
    print("=" * 60)
    print("COI Disclosure Review System")
    print("=" * 60)
    print()
    print("Starting server...")
    print()
    print("📊 Main Viewer: http://localhost:8000")
    print("📈 Dashboard: http://localhost:8000/dashboard")
    print("🔧 API Docs: http://localhost:8000/api/docs")
    print("📚 ReDoc: http://localhost:8000/api/redoc")
    print()
    print("Press Ctrl+C to stop the server")
    print("=" * 60)
    
    # Run server
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )