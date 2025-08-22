"""
Vercel-compatible entry point for COI Disclosure Review System
Adapts FastAPI application for serverless deployment
"""
import os
import sys
from pathlib import Path

# Add app directory to Python path
app_dir = Path(__file__).parent.parent / "app"
sys.path.insert(0, str(app_dir))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum

# Import our existing API application
from api.main import create_api_app
from api.services.policy_engine import PolicyEngine

# Create the FastAPI application
app = FastAPI(
    title="COI Disclosure Review API",
    description="API for managing COI disclosures and policy compliance",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)

# Configure CORS for Vercel deployment
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount the existing API application
api_app = create_api_app()
app.mount("/api/v1", api_app)

# Health check endpoint
@app.get("/api/health")
async def health_check():
    """Health check endpoint for monitoring"""
    return {
        "status": "healthy",
        "service": "COI Disclosure Review System",
        "version": "1.0.0",
        "deployment": "vercel"
    }

# Policy engine status endpoint
@app.get("/api/policy-engine/status")
async def policy_engine_status():
    """Check Policy Decision Engine status"""
    try:
        engine = PolicyEngine()
        return {
            "status": "operational",
            "thresholds_loaded": engine.thresholds is not None,
            "policies_loaded": engine.policies is not None,
            "ready": True
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "ready": False
        }

# Create handler for Vercel
handler = Mangum(app)

# Export for Vercel
def vc_handler(request, context):
    """Vercel serverless function handler"""
    return handler(request, context)