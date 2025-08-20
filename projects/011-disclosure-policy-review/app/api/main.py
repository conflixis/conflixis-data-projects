"""
FastAPI application factory for COI Disclosure Review API
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.routes import disclosures_router, policies_router, stats_router
from api.routes.interpretation import router as interpretation_router
from api.routes.criteria import router as criteria_router
from api.routes.evaluation_rules import router as evaluation_router


def create_api_app() -> FastAPI:
    """Create and configure the FastAPI application
    
    Returns:
        Configured FastAPI application
    """
    app = FastAPI(
        title="COI Disclosure Review API",
        description="API for managing and reviewing Conflict of Interest disclosures",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json"
    )
    
    # Configure CORS for frontend access
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # In production, specify actual origins
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Include routers (no prefix needed since this app is mounted at /api)
    app.include_router(disclosures_router)
    app.include_router(policies_router)
    app.include_router(stats_router)
    app.include_router(interpretation_router)
    app.include_router(criteria_router)
    app.include_router(evaluation_router)
    
    # Root endpoint
    @app.get("/")
    async def root():
        """Root endpoint"""
        return {
            "name": "COI Disclosure Review API",
            "version": "1.0.0",
            "documentation": "/docs",
            "api_base": "/api"
        }
    
    
    return app