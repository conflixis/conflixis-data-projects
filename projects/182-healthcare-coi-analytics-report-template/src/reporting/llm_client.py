"""
LLM Client Module for Report Generation
Handles interaction with Claude API for narrative generation
"""

import os
import json
import logging
from typing import Dict, Any, Optional, List
import pandas as pd
from anthropic import Anthropic
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class ClaudeLLMClient:
    """Client for interacting with Claude API for report generation"""
    
    def __init__(self, model: str = "claude-3-opus-20240229"):
        """
        Initialize Claude client
        
        Args:
            model: Claude model to use
        """
        api_key = os.getenv('CLAUDE_API_KEY')
        if not api_key:
            raise ValueError("CLAUDE_API_KEY not found in environment")
        
        self.client = Anthropic(api_key=api_key)
        self.model = model
        self.max_retries = 3
        self.retry_delay = 2
        
        logger.info(f"Initialized Claude LLM client with model: {model}")
    
    def generate_section(
        self,
        section_config: Dict[str, Any],
        section_data: Dict[str, Any],
        previous_sections: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Generate narrative for a report section
        
        Args:
            section_config: Configuration for this section from prompts YAML
            section_data: Data required for this section
            previous_sections: Previously generated sections (for context)
            
        Returns:
            Generated narrative text
        """
        # Format the prompt with data
        prompt = section_config['prompt'].format(
            data=self._format_data_for_prompt(section_data)
        )
        
        # Add previous sections context if this is executive summary
        if previous_sections and 'previous_sections' in section_config['prompt']:
            sections_summary = self._summarize_previous_sections(previous_sections)
            prompt = prompt.replace('{previous_sections}', sections_summary)
        
        # Add system message for style
        system_message = """You are an investigative healthcare journalist writing a data-driven report 
        about conflicts of interest in healthcare. Your style should be compelling and narrative-driven, 
        similar to ProPublica or Wall Street Journal investigations. Use specific numbers and avoid 
        academic language or citations.
        
        CRITICAL: The data provided is from years 2020-2024. You MUST use these actual years in your narrative 
        and tables. DO NOT use years like 2014-2018 or any other years not present in the data.
        
        IMPORTANT: You MUST include markdown tables as specified in the prompts. Tables should:
        - Have clear headers with pipes (|) as separators
        - Include separator line with dashes (e.g., |---|---|---|)
        - Be integrated naturally within the narrative, not just appended at the end
        - Show actual data values from the provided data
        - Use the EXACT years from the data (2020-2024)
        - Be properly formatted for markdown rendering"""
        
        # Generate with retries
        for attempt in range(self.max_retries):
            try:
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=section_config.get('constraints', {}).get('max_length', 500) * 4,  # Approximate tokens
                    temperature=0.7,
                    system=system_message,
                    messages=[
                        {"role": "user", "content": prompt}
                    ]
                )
                
                narrative = response.content[0].text
                logger.info(f"Generated {section_config.get('context', 'section')} ({len(narrative)} chars)")
                
                return narrative
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    logger.error(f"Failed to generate section after {self.max_retries} attempts")
                    raise
    
    def _format_data_for_prompt(self, data: Dict[str, Any]) -> str:
        """
        Format data dictionary for inclusion in prompt
        
        Args:
            data: Data dictionary
            
        Returns:
            Formatted string representation
        """
        formatted_parts = []
        
        for key, value in data.items():
            if isinstance(value, dict):
                # Format nested dictionaries
                formatted_parts.append(f"\n{key.replace('_', ' ').title()}:")
                for sub_key, sub_value in value.items():
                    if isinstance(sub_value, (int, float)):
                        # Format numbers
                        if sub_value > 1000000:
                            formatted_value = f"${sub_value/1000000:.1f}M"
                        elif sub_value > 1000:
                            formatted_value = f"${sub_value/1000:.1f}K"
                        else:
                            formatted_value = f"{sub_value:,.2f}"
                    else:
                        formatted_value = str(sub_value)
                    
                    formatted_parts.append(f"  - {sub_key}: {formatted_value}")
            
            elif hasattr(value, 'to_dict'):
                # Handle DataFrames - format as table-ready data
                formatted_parts.append(f"\n{key.replace('_', ' ').title()} (DataFrame with {len(value)} rows):")
                
                # Show column names
                if not value.empty:
                    cols = list(value.columns)
                    formatted_parts.append(f"  Columns: {', '.join(cols)}")
                    
                    # Check if index has meaningful name (like payment_year)
                    if value.index.name:
                        formatted_parts.append(f"  Index: {value.index.name}")
                        # Reset index to include it as a column in the data
                        value_with_index = value.reset_index()
                        df_dict = value_with_index.head(10).to_dict('records')
                    else:
                        df_dict = value.head(10).to_dict('records')
                    
                    formatted_parts.append("  Sample data (first 10 rows):")
                    for i, row in enumerate(df_dict, 1):
                        formatted_parts.append(f"    Row {i}: {self._format_row(row)}")
            
            else:
                # Simple values
                formatted_parts.append(f"{key}: {value}")
        
        return "\n".join(formatted_parts)
    
    def _format_row(self, row: Dict) -> str:
        """Format a data row for display"""
        parts = []
        for k, v in row.items():
            # Format column name
            col_name = k.replace('_', ' ').title() if '_' in k else k
            
            # Format value
            if pd.isna(v):
                formatted_v = "N/A"
            elif k in ['payment_year', 'year', 'Year']:
                # Keep years as integers
                formatted_v = str(int(v))
            elif isinstance(v, (int, float)):
                if k.lower() in ['providers', 'count', 'transactions']:
                    formatted_v = f"{int(v):,}"
                elif v > 1000000:
                    formatted_v = f"${v/1000000:.1f}M"
                elif v > 1000:
                    formatted_v = f"${v:,.0f}"
                elif v > 1:
                    formatted_v = f"{v:.2f}"
                else:
                    formatted_v = f"{v:.1%}" if v < 1 else str(v)
            else:
                formatted_v = str(v)
            
            parts.append(f"{col_name}: {formatted_v}")
        return " | ".join(parts)
    
    def _summarize_previous_sections(self, sections: Dict[str, str]) -> str:
        """
        Summarize previous sections for executive summary context
        
        Args:
            sections: Dictionary of section_name: narrative
            
        Returns:
            Summary of key findings
        """
        summary_parts = ["Key findings from analysis sections:"]
        
        for section_name, narrative in sections.items():
            if section_name != 'executive_summary':
                # Extract first significant sentence or finding
                lines = narrative.split('\n')
                for line in lines:
                    if any(indicator in line.lower() for indicator in 
                           ['increase', 'correlation', 'finding', 'reveals', 'demonstrates']):
                        summary_parts.append(f"- {section_name}: {line.strip()}")
                        break
        
        return "\n".join(summary_parts)