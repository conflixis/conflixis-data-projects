"""
Logging Configuration Module
DA-173: Provider Profile Web Enrichment POC
"""

import os
import logging
import json
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from typing import Dict, Any, Optional

def setup_logger(
    name: str, 
    config: Dict[str, Any],
    log_dir: Optional[Path] = None
) -> logging.Logger:
    """
    Setup a logger with the specified configuration.
    
    Args:
        name: Logger name
        config: Logging configuration dictionary
        log_dir: Optional specific log directory
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Set log level
    level = getattr(logging, config.get("level", "INFO"))
    logger.setLevel(level)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Create log directory if needed
    if log_dir is None:
        log_dir = Path(config.get("log_dir", "data/logs"))
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_formatter = logging.Formatter(
        config.get("log_format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # File handler with rotation
    log_file = log_dir / f"{name.lower().replace(' ', '_')}.log"
    
    if config.get("log_rotation") == "daily":
        file_handler = TimedRotatingFileHandler(
            log_file,
            when="midnight",
            interval=1,
            backupCount=config.get("backup_count", 30),
            encoding="utf-8"
        )
    elif config.get("log_rotation") == "weekly":
        file_handler = TimedRotatingFileHandler(
            log_file,
            when="W0",  # Weekly on Monday
            interval=1,
            backupCount=config.get("backup_count", 30),
            encoding="utf-8"
        )
    else:  # Size-based rotation
        max_bytes = config.get("max_log_size_mb", 100) * 1024 * 1024
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=config.get("backup_count", 30),
            encoding="utf-8"
        )
    
    file_handler.setLevel(level)
    file_formatter = logging.Formatter(
        config.get("log_format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # Add JSON file handler for structured logs
    json_log_file = log_dir / f"{name.lower().replace(' ', '_')}_structured.jsonl"
    json_handler = RotatingFileHandler(
        json_log_file,
        maxBytes=config.get("max_log_size_mb", 100) * 1024 * 1024,
        backupCount=config.get("backup_count", 30),
        encoding="utf-8"
    )
    json_handler.setLevel(level)
    json_handler.setFormatter(JsonFormatter())
    logger.addHandler(json_handler)
    
    return logger


class JsonFormatter(logging.Formatter):
    """
    Custom JSON formatter for structured logging.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON.
        
        Args:
            record: Log record to format
            
        Returns:
            JSON formatted string
        """
        log_obj = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        
        # Add extra fields if present
        if hasattr(record, "request_id"):
            log_obj["request_id"] = record.request_id
        
        if hasattr(record, "provider_name"):
            log_obj["provider_name"] = record.provider_name
        
        if hasattr(record, "error_type"):
            log_obj["error_type"] = record.error_type
        
        # Add exception info if present
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_obj, ensure_ascii=False)


class AuditLogger:
    """
    Specialized logger for audit trails of web search requests and responses.
    """
    
    def __init__(self, audit_dir: Path):
        """
        Initialize audit logger.
        
        Args:
            audit_dir: Directory for audit logs
        """
        self.audit_dir = Path(audit_dir)
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        
    def log_request(
        self,
        request_id: str,
        provider_info: Dict[str, Any],
        config: Dict[str, Any]
    ):
        """
        Log a web search request.
        
        Args:
            request_id: Unique request identifier
            provider_info: Provider information being searched
            config: Configuration used for the request
        """
        timestamp = datetime.now()
        
        audit_entry = {
            "request_id": request_id,
            "timestamp": timestamp.isoformat(),
            "type": "request",
            "provider_info": provider_info,
            "config": {
                "model": config.get("model"),
                "temperature": config.get("temperature"),
                "max_tokens": config.get("max_tokens"),
                "search_context_size": config.get("search_context_size")
            }
        }
        
        # Save to daily audit file
        audit_file = self.audit_dir / f"audit_{timestamp.strftime('%Y%m%d')}.jsonl"
        
        with open(audit_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(audit_entry, ensure_ascii=False) + "\n")
    
    def log_response(
        self,
        request_id: str,
        response_summary: Dict[str, Any],
        citations_count: int,
        confidence: float,
        processing_time: float
    ):
        """
        Log a web search response.
        
        Args:
            request_id: Unique request identifier
            response_summary: Summary of the response
            citations_count: Number of citations found
            confidence: Overall confidence score
            processing_time: Time taken to process
        """
        timestamp = datetime.now()
        
        audit_entry = {
            "request_id": request_id,
            "timestamp": timestamp.isoformat(),
            "type": "response",
            "summary": response_summary,
            "metrics": {
                "citations_count": citations_count,
                "confidence": confidence,
                "processing_time_seconds": processing_time
            }
        }
        
        # Save to daily audit file
        audit_file = self.audit_dir / f"audit_{timestamp.strftime('%Y%m%d')}.jsonl"
        
        with open(audit_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(audit_entry, ensure_ascii=False) + "\n")
    
    def log_error(
        self,
        request_id: str,
        error_type: str,
        error_message: str,
        stack_trace: Optional[str] = None
    ):
        """
        Log an error during processing.
        
        Args:
            request_id: Unique request identifier
            error_type: Type of error
            error_message: Error message
            stack_trace: Optional stack trace
        """
        timestamp = datetime.now()
        
        audit_entry = {
            "request_id": request_id,
            "timestamp": timestamp.isoformat(),
            "type": "error",
            "error": {
                "type": error_type,
                "message": error_message,
                "stack_trace": stack_trace
            }
        }
        
        # Save to daily audit file
        audit_file = self.audit_dir / f"audit_{timestamp.strftime('%Y%m%d')}.jsonl"
        
        with open(audit_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(audit_entry, ensure_ascii=False) + "\n")
    
    def get_request_history(self, request_id: str) -> List[Dict[str, Any]]:
        """
        Retrieve full history for a specific request.
        
        Args:
            request_id: Request ID to retrieve
            
        Returns:
            List of audit entries for the request
        """
        history = []
        
        # Search through all audit files
        for audit_file in sorted(self.audit_dir.glob("audit_*.jsonl")):
            with open(audit_file, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        entry = json.loads(line)
                        if entry.get("request_id") == request_id:
                            history.append(entry)
                    except json.JSONDecodeError:
                        continue
        
        return sorted(history, key=lambda x: x.get("timestamp", ""))


class MetricsLogger:
    """
    Logger for tracking metrics and statistics.
    """
    
    def __init__(self, metrics_file: Path):
        """
        Initialize metrics logger.
        
        Args:
            metrics_file: Path to metrics file
        """
        self.metrics_file = Path(metrics_file)
        self.metrics_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize or load existing metrics
        if self.metrics_file.exists():
            with open(self.metrics_file, "r") as f:
                self.metrics = json.load(f)
        else:
            self.metrics = {
                "total_requests": 0,
                "successful_requests": 0,
                "failed_requests": 0,
                "total_processing_time": 0,
                "total_citations": 0,
                "total_tokens_used": 0,
                "providers_processed": [],
                "error_types": {},
                "source_type_distribution": {},
                "confidence_scores": []
            }
    
    def log_request_metrics(
        self,
        request_id: str,
        provider_name: str,
        success: bool,
        processing_time: float,
        citations_count: int,
        tokens_used: int,
        confidence: float,
        source_types: Dict[str, int],
        error_type: Optional[str] = None
    ):
        """
        Log metrics for a request.
        
        Args:
            request_id: Request identifier
            provider_name: Provider name
            success: Whether request was successful
            processing_time: Processing time in seconds
            citations_count: Number of citations
            tokens_used: Approximate tokens used
            confidence: Overall confidence score
            source_types: Distribution of source types
            error_type: Type of error if failed
        """
        self.metrics["total_requests"] += 1
        
        if success:
            self.metrics["successful_requests"] += 1
        else:
            self.metrics["failed_requests"] += 1
            if error_type:
                self.metrics["error_types"][error_type] = \
                    self.metrics["error_types"].get(error_type, 0) + 1
        
        self.metrics["total_processing_time"] += processing_time
        self.metrics["total_citations"] += citations_count
        self.metrics["total_tokens_used"] += tokens_used
        
        # Track unique providers
        if provider_name not in self.metrics["providers_processed"]:
            self.metrics["providers_processed"].append(provider_name)
        
        # Update source type distribution
        for source_type, count in source_types.items():
            self.metrics["source_type_distribution"][source_type] = \
                self.metrics["source_type_distribution"].get(source_type, 0) + count
        
        # Track confidence scores
        self.metrics["confidence_scores"].append(confidence)
        
        # Calculate averages
        if self.metrics["total_requests"] > 0:
            self.metrics["avg_processing_time"] = \
                self.metrics["total_processing_time"] / self.metrics["total_requests"]
            self.metrics["avg_citations_per_request"] = \
                self.metrics["total_citations"] / self.metrics["total_requests"]
            self.metrics["avg_confidence"] = \
                sum(self.metrics["confidence_scores"]) / len(self.metrics["confidence_scores"])
            self.metrics["success_rate"] = \
                self.metrics["successful_requests"] / self.metrics["total_requests"]
        
        # Save metrics
        self.save_metrics()
    
    def save_metrics(self):
        """Save metrics to file."""
        with open(self.metrics_file, "w") as f:
            json.dump(self.metrics, f, indent=2, ensure_ascii=False)
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary of metrics.
        
        Returns:
            Dictionary with key metrics
        """
        return {
            "total_requests": self.metrics["total_requests"],
            "success_rate": self.metrics.get("success_rate", 0),
            "avg_processing_time": self.metrics.get("avg_processing_time", 0),
            "avg_citations": self.metrics.get("avg_citations_per_request", 0),
            "avg_confidence": self.metrics.get("avg_confidence", 0),
            "unique_providers": len(self.metrics["providers_processed"]),
            "total_tokens_used": self.metrics["total_tokens_used"]
        }