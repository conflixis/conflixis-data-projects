"""
Visualization Generator Module
Creates charts and graphs for reports
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10


class VisualizationGenerator:
    """Generate visualizations for healthcare COI analysis"""
    
    def __init__(self, output_dir: str = "reports/figures"):
        """
        Initialize visualization generator
        
        Args:
            output_dir: Directory to save generated figures
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Conflixis color palette
        self.colors = {
            'primary': '#0c343a',
            'secondary': '#eab96d',
            'accent': '#4c94ed',
            'success': '#28a745',
            'warning': '#ffc107',
            'danger': '#dc3545',
            'gradient': ['#0c343a', '#1a4d55', '#2a6670', '#4c94ed', '#eab96d']
        }
    
    def generate_all_visualizations(self, analysis_results: Dict[str, Any]) -> List[str]:
        """
        Generate all standard visualizations
        
        Args:
            analysis_results: Dictionary containing all analysis results
            
        Returns:
            List of paths to generated figures
        """
        figures = []
        
        # Open Payments visualizations
        if 'open_payments' in analysis_results:
            figures.extend(self.generate_payment_visualizations(analysis_results['open_payments']))
        
        # Prescription visualizations
        if 'prescriptions' in analysis_results:
            figures.extend(self.generate_prescription_visualizations(analysis_results['prescriptions']))
        
        # Correlation visualizations
        if 'correlations' in analysis_results:
            figures.extend(self.generate_correlation_visualizations(analysis_results['correlations']))
        
        # Risk visualizations
        if 'risk_assessment' in analysis_results:
            figures.extend(self.generate_risk_visualizations(analysis_results['risk_assessment']))
        
        logger.info(f"Generated {len(figures)} visualizations")
        return figures
    
    def generate_payment_visualizations(self, payment_data: Dict[str, Any]) -> List[str]:
        """Generate Open Payments visualizations"""
        figures = []
        
        # Payment distribution histogram
        if 'payment_distribution' in payment_data:
            fig_path = self._create_payment_distribution_chart(payment_data['payment_distribution'])
            if fig_path:
                figures.append(fig_path)
        
        # Yearly trends line chart
        if 'yearly_trends' in payment_data and not payment_data['yearly_trends'].empty:
            fig_path = self._create_yearly_trends_chart(payment_data['yearly_trends'])
            if fig_path:
                figures.append(fig_path)
        
        # Top manufacturers bar chart
        if 'top_manufacturers' in payment_data and not payment_data['top_manufacturers'].empty:
            fig_path = self._create_manufacturers_chart(payment_data['top_manufacturers'])
            if fig_path:
                figures.append(fig_path)
        
        # Payment categories pie chart
        if 'payment_categories' in payment_data and not payment_data['payment_categories'].empty:
            fig_path = self._create_categories_pie_chart(payment_data['payment_categories'])
            if fig_path:
                figures.append(fig_path)
        
        return figures
    
    def generate_prescription_visualizations(self, rx_data: Dict[str, Any]) -> List[str]:
        """Generate prescription visualizations"""
        figures = []
        
        # Top drugs bar chart
        if 'top_drugs' in rx_data and not rx_data['top_drugs'].empty:
            fig_path = self._create_top_drugs_chart(rx_data['top_drugs'])
            if fig_path:
                figures.append(fig_path)
        
        # Prescription trends over time
        if 'yearly_trends' in rx_data and not rx_data['yearly_trends'].empty:
            fig_path = self._create_rx_trends_chart(rx_data['yearly_trends'])
            if fig_path:
                figures.append(fig_path)
        
        return figures
    
    def generate_correlation_visualizations(self, corr_data: Dict[str, Any]) -> List[str]:
        """Generate correlation visualizations"""
        figures = []
        
        # Influence factors bar chart
        if 'drug_specific' in corr_data and not corr_data['drug_specific'].empty:
            fig_path = self._create_influence_factors_chart(corr_data['drug_specific'])
            if fig_path:
                figures.append(fig_path)
        
        # Payment tier effects
        if 'payment_tiers' in corr_data and not corr_data['payment_tiers'].empty:
            fig_path = self._create_payment_tier_chart(corr_data['payment_tiers'])
            if fig_path:
                figures.append(fig_path)
        
        # Provider vulnerability comparison
        if 'provider_type_vulnerability' in corr_data and not corr_data['provider_type_vulnerability'].empty:
            fig_path = self._create_vulnerability_chart(corr_data['provider_type_vulnerability'])
            if fig_path:
                figures.append(fig_path)
        
        return figures
    
    def generate_risk_visualizations(self, risk_data: Dict[str, Any]) -> List[str]:
        """Generate risk assessment visualizations"""
        figures = []
        
        # Risk distribution
        if 'distribution' in risk_data:
            fig_path = self._create_risk_distribution_chart(risk_data['distribution'])
            if fig_path:
                figures.append(fig_path)
        
        # Risk heatmap
        if 'top_risks' in risk_data:
            fig_path = self._create_risk_heatmap(risk_data['top_risks'])
            if fig_path:
                figures.append(fig_path)
        
        return figures
    
    def _create_payment_distribution_chart(self, distribution_data: Dict) -> Optional[str]:
        """Create payment distribution histogram"""
        try:
            if 'tiers' not in distribution_data:
                return None
            
            tiers_df = distribution_data['tiers']
            
            fig, ax = plt.subplots(figsize=(12, 6))
            
            bars = ax.bar(
                tiers_df['tier'],
                tiers_df['count'],
                color=self.colors['gradient'][:len(tiers_df)]
            )
            
            ax.set_xlabel('Payment Tier', fontsize=12)
            ax.set_ylabel('Number of Transactions', fontsize=12)
            ax.set_title('Distribution of Industry Payments by Amount', fontsize=14, fontweight='bold')
            
            # Add value labels on bars
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height):,}',
                       ha='center', va='bottom')
            
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            fig_path = self.output_dir / 'payment_distribution.png'
            plt.savefig(fig_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(fig_path)
            
        except Exception as e:
            logger.error(f"Failed to create payment distribution chart: {e}")
            return None
    
    def _create_yearly_trends_chart(self, trends_df: pd.DataFrame) -> Optional[str]:
        """Create yearly trends line chart"""
        try:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
            
            # Payment trends
            years = trends_df.index
            ax1.plot(years, trends_df['total_payments'], 
                    marker='o', linewidth=2, color=self.colors['primary'])
            ax1.fill_between(years, trends_df['total_payments'], 
                            alpha=0.3, color=self.colors['primary'])
            ax1.set_xlabel('Year', fontsize=12)
            ax1.set_ylabel('Total Payments ($)', fontsize=12)
            ax1.set_title('Industry Payments Over Time', fontsize=14, fontweight='bold')
            ax1.grid(True, alpha=0.3)
            
            # Provider count trends
            ax2.plot(years, trends_df['providers'], 
                    marker='s', linewidth=2, color=self.colors['accent'])
            ax2.fill_between(years, trends_df['providers'], 
                            alpha=0.3, color=self.colors['accent'])
            ax2.set_xlabel('Year', fontsize=12)
            ax2.set_ylabel('Number of Providers', fontsize=12)
            ax2.set_title('Providers Receiving Payments Over Time', fontsize=14, fontweight='bold')
            ax2.grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            fig_path = self.output_dir / 'yearly_trends.png'
            plt.savefig(fig_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(fig_path)
            
        except Exception as e:
            logger.error(f"Failed to create yearly trends chart: {e}")
            return None
    
    def _create_manufacturers_chart(self, manufacturers_df: pd.DataFrame) -> Optional[str]:
        """Create top manufacturers bar chart"""
        try:
            top_10 = manufacturers_df.head(10)
            
            fig, ax = plt.subplots(figsize=(12, 8))
            
            bars = ax.barh(
                range(len(top_10)),
                top_10['total_payments'],
                color=self.colors['gradient'][0]
            )
            
            ax.set_yticks(range(len(top_10)))
            ax.set_yticklabels(top_10.index)
            ax.set_xlabel('Total Payments ($)', fontsize=12)
            ax.set_title('Top 10 Manufacturers by Payment Volume', fontsize=14, fontweight='bold')
            
            # Add value labels
            for i, bar in enumerate(bars):
                width = bar.get_width()
                ax.text(width, bar.get_y() + bar.get_height()/2.,
                       f'${width/1e6:.1f}M',
                       ha='left', va='center')
            
            plt.tight_layout()
            
            fig_path = self.output_dir / 'top_manufacturers.png'
            plt.savefig(fig_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(fig_path)
            
        except Exception as e:
            logger.error(f"Failed to create manufacturers chart: {e}")
            return None
    
    def _create_categories_pie_chart(self, categories_df: pd.DataFrame) -> Optional[str]:
        """Create payment categories pie chart"""
        try:
            top_categories = categories_df.head(8)
            other_amount = categories_df.iloc[8:]['total_amount'].sum() if len(categories_df) > 8 else 0
            
            if other_amount > 0:
                top_categories = top_categories.append(
                    pd.Series({'total_amount': other_amount}, name='Other')
                )
            
            fig, ax = plt.subplots(figsize=(10, 8))
            
            wedges, texts, autotexts = ax.pie(
                top_categories['total_amount'],
                labels=top_categories.index,
                autopct='%1.1f%%',
                colors=self.colors['gradient'] * 2,
                startangle=90
            )
            
            ax.set_title('Payment Distribution by Category', fontsize=14, fontweight='bold')
            
            # Enhance text
            for text in texts:
                text.set_fontsize(10)
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontsize(10)
                autotext.set_fontweight('bold')
            
            plt.tight_layout()
            
            fig_path = self.output_dir / 'payment_categories.png'
            plt.savefig(fig_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(fig_path)
            
        except Exception as e:
            logger.error(f"Failed to create categories pie chart: {e}")
            return None
    
    def _create_influence_factors_chart(self, drug_df: pd.DataFrame) -> Optional[str]:
        """Create influence factors bar chart"""
        try:
            top_10 = drug_df.nlargest(10, 'influence_factor')
            
            fig, ax = plt.subplots(figsize=(12, 8))
            
            bars = ax.barh(
                range(len(top_10)),
                top_10['influence_factor'],
                color=[self.colors['danger'] if x > 100 else self.colors['warning'] if x > 10 else self.colors['primary'] 
                       for x in top_10['influence_factor']]
            )
            
            ax.set_yticks(range(len(top_10)))
            ax.set_yticklabels(top_10['drug'])
            ax.set_xlabel('Influence Factor (x)', fontsize=12)
            ax.set_title('Top 10 Drugs by Payment Influence Factor', fontsize=14, fontweight='bold')
            ax.axvline(x=1, color='gray', linestyle='--', alpha=0.5, label='No influence')
            
            # Add value labels
            for i, bar in enumerate(bars):
                width = bar.get_width()
                ax.text(width, bar.get_y() + bar.get_height()/2.,
                       f'{width:.0f}x',
                       ha='left', va='center')
            
            ax.legend()
            plt.tight_layout()
            
            fig_path = self.output_dir / 'influence_factors.png'
            plt.savefig(fig_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(fig_path)
            
        except Exception as e:
            logger.error(f"Failed to create influence factors chart: {e}")
            return None
    
    def _create_payment_tier_chart(self, tier_df: pd.DataFrame) -> Optional[str]:
        """Create payment tier effects chart"""
        try:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
            
            # Average prescription cost by tier
            ax1.bar(
                tier_df['payment_tier'],
                tier_df['avg_rx_cost'],
                color=self.colors['gradient'][:len(tier_df)]
            )
            ax1.set_xlabel('Payment Tier', fontsize=12)
            ax1.set_ylabel('Average Prescription Cost ($)', fontsize=12)
            ax1.set_title('Prescription Cost by Payment Tier', fontsize=14, fontweight='bold')
            ax1.tick_params(axis='x', rotation=45)
            
            # ROI by tier (if available)
            if 'roi' in tier_df.columns:
                tier_with_roi = tier_df[tier_df['roi'].notna()]
                ax2.bar(
                    tier_with_roi['payment_tier'],
                    tier_with_roi['roi'],
                    color=self.colors['secondary']
                )
                ax2.set_xlabel('Payment Tier', fontsize=12)
                ax2.set_ylabel('Return on Investment (x)', fontsize=12)
                ax2.set_title('ROI by Payment Tier', fontsize=14, fontweight='bold')
                ax2.tick_params(axis='x', rotation=45)
            
            plt.tight_layout()
            
            fig_path = self.output_dir / 'payment_tier_effects.png'
            plt.savefig(fig_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(fig_path)
            
        except Exception as e:
            logger.error(f"Failed to create payment tier chart: {e}")
            return None
    
    def create_custom_visualization(
        self,
        data: pd.DataFrame,
        chart_type: str,
        title: str,
        xlabel: str = None,
        ylabel: str = None,
        filename: str = None
    ) -> str:
        """
        Create custom visualization
        
        Args:
            data: DataFrame with data to visualize
            chart_type: Type of chart (bar, line, scatter, heatmap)
            title: Chart title
            xlabel: X-axis label
            ylabel: Y-axis label
            filename: Output filename
            
        Returns:
            Path to saved figure
        """
        fig, ax = plt.subplots(figsize=(12, 6))
        
        if chart_type == 'bar':
            data.plot(kind='bar', ax=ax, color=self.colors['primary'])
        elif chart_type == 'line':
            data.plot(kind='line', ax=ax, marker='o', color=self.colors['accent'])
        elif chart_type == 'scatter':
            if len(data.columns) >= 2:
                ax.scatter(data.iloc[:, 0], data.iloc[:, 1], 
                          color=self.colors['primary'], alpha=0.6)
        elif chart_type == 'heatmap':
            sns.heatmap(data, annot=True, fmt='.2f', cmap='RdYlBu_r', ax=ax)
        
        ax.set_title(title, fontsize=14, fontweight='bold')
        if xlabel:
            ax.set_xlabel(xlabel, fontsize=12)
        if ylabel:
            ax.set_ylabel(ylabel, fontsize=12)
        
        plt.tight_layout()
        
        if not filename:
            filename = f"custom_{chart_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        
        fig_path = self.output_dir / filename
        plt.savefig(fig_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return str(fig_path)
    
    def _create_vulnerability_chart(self, vuln_df: pd.DataFrame) -> Optional[str]:
        """Create provider vulnerability comparison chart"""
        try:
            fig, ax = plt.subplots(figsize=(10, 6))
            
            x = range(len(vuln_df))
            width = 0.35
            
            bars1 = ax.bar([i - width/2 for i in x], vuln_df['avg_rx_with_payments'],
                          width, label='With Payments', color=self.colors['danger'])
            bars2 = ax.bar([i + width/2 for i in x], vuln_df['avg_rx_without_payments'],
                          width, label='Without Payments', color=self.colors['success'])
            
            ax.set_xlabel('Provider Type', fontsize=12)
            ax.set_ylabel('Average Prescription Cost ($)', fontsize=12)
            ax.set_title('Provider Type Vulnerability to Payment Influence', fontsize=14, fontweight='bold')
            ax.set_xticks(x)
            ax.set_xticklabels(vuln_df['provider_type'])
            ax.legend()
            
            plt.tight_layout()
            
            fig_path = self.output_dir / 'provider_vulnerability.png'
            plt.savefig(fig_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(fig_path)
            
        except Exception as e:
            logger.error(f"Failed to create vulnerability chart: {e}")
            return None
    
    def _create_top_drugs_chart(self, drugs_df: pd.DataFrame) -> Optional[str]:
        """Create top prescribed drugs chart"""
        try:
            top_10 = drugs_df.head(10)
            
            fig, ax = plt.subplots(figsize=(12, 8))
            
            bars = ax.barh(
                range(len(top_10)),
                top_10['total_cost'],
                color=self.colors['accent']
            )
            
            ax.set_yticks(range(len(top_10)))
            ax.set_yticklabels(top_10.index)
            ax.set_xlabel('Total Prescription Cost ($)', fontsize=12)
            ax.set_title('Top 10 Prescribed Drugs by Value', fontsize=14, fontweight='bold')
            
            # Add value labels
            for i, bar in enumerate(bars):
                width = bar.get_width()
                ax.text(width, bar.get_y() + bar.get_height()/2.,
                       f'${width/1e6:.1f}M',
                       ha='left', va='center')
            
            plt.tight_layout()
            
            fig_path = self.output_dir / 'top_drugs.png'
            plt.savefig(fig_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(fig_path)
            
        except Exception as e:
            logger.error(f"Failed to create top drugs chart: {e}")
            return None
    
    def _create_rx_trends_chart(self, trends_df: pd.DataFrame) -> Optional[str]:
        """Create prescription trends chart"""
        try:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
            
            years = trends_df.index
            
            # Cost trends
            ax1.plot(years, trends_df['total_cost'], 
                    marker='o', linewidth=2, color=self.colors['accent'])
            ax1.fill_between(years, trends_df['total_cost'], 
                            alpha=0.3, color=self.colors['accent'])
            ax1.set_xlabel('Year', fontsize=12)
            ax1.set_ylabel('Total Cost ($)', fontsize=12)
            ax1.set_title('Prescription Costs Over Time', fontsize=14, fontweight='bold')
            ax1.grid(True, alpha=0.3)
            
            # Volume trends
            ax2.plot(years, trends_df['total_claims'], 
                    marker='s', linewidth=2, color=self.colors['primary'])
            ax2.fill_between(years, trends_df['total_claims'], 
                            alpha=0.3, color=self.colors['primary'])
            ax2.set_xlabel('Year', fontsize=12)
            ax2.set_ylabel('Total Claims', fontsize=12)
            ax2.set_title('Prescription Volume Over Time', fontsize=14, fontweight='bold')
            ax2.grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            fig_path = self.output_dir / 'rx_trends.png'
            plt.savefig(fig_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(fig_path)
            
        except Exception as e:
            logger.error(f"Failed to create prescription trends chart: {e}")
            return None
    
    def _create_risk_distribution_chart(self, distribution: List[Dict]) -> Optional[str]:
        """Create risk distribution chart"""
        try:
            dist_df = pd.DataFrame(distribution)
            
            # Filter out summary row if present
            dist_df = dist_df[dist_df['risk_level'] != 'Mean Score']
            
            fig, ax = plt.subplots(figsize=(10, 6))
            
            colors_map = {
                'Low': self.colors['success'],
                'Medium': self.colors['warning'],
                'High': '#ff8c00',
                'Critical': self.colors['danger'],
                'Extreme': '#8b0000'
            }
            
            bars = ax.bar(
                dist_df['risk_level'],
                dist_df['provider_count'],
                color=[colors_map.get(level, self.colors['primary']) for level in dist_df['risk_level']]
            )
            
            ax.set_xlabel('Risk Level', fontsize=12)
            ax.set_ylabel('Number of Providers', fontsize=12)
            ax.set_title('Provider Risk Distribution', fontsize=14, fontweight='bold')
            
            # Add value labels and percentages
            for i, bar in enumerate(bars):
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{int(height):,}\n({dist_df.iloc[i]["percentage"]:.1f}%)',
                       ha='center', va='bottom')
            
            plt.tight_layout()
            
            fig_path = self.output_dir / 'risk_distribution.png'
            plt.savefig(fig_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(fig_path)
            
        except Exception as e:
            logger.error(f"Failed to create risk distribution chart: {e}")
            return None
    
    def _create_risk_heatmap(self, top_risks: List[Dict]) -> Optional[str]:
        """Create risk component heatmap for top risk providers"""
        try:
            if not top_risks or len(top_risks) == 0:
                return None
            
            # Extract risk components
            risk_components = ['payment_risk', 'prescription_risk', 'relationship_risk']
            
            # Create matrix for heatmap
            providers = []
            data_matrix = []
            
            for risk in top_risks[:20]:  # Top 20 providers
                if all(comp in risk for comp in risk_components):
                    providers.append(risk.get('NPI', 'Unknown'))
                    data_matrix.append([risk[comp] for comp in risk_components])
            
            if not data_matrix:
                return None
            
            fig, ax = plt.subplots(figsize=(10, 8))
            
            sns.heatmap(
                data_matrix,
                annot=True,
                fmt='.0f',
                cmap='RdYlBu_r',
                xticklabels=risk_components,
                yticklabels=providers,
                ax=ax,
                vmin=0,
                vmax=100
            )
            
            ax.set_title('Risk Component Heatmap - Top Risk Providers', fontsize=14, fontweight='bold')
            ax.set_xlabel('Risk Components', fontsize=12)
            ax.set_ylabel('Provider NPI', fontsize=12)
            
            plt.tight_layout()
            
            fig_path = self.output_dir / 'risk_heatmap.png'
            plt.savefig(fig_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(fig_path)
            
        except Exception as e:
            logger.error(f"Failed to create risk heatmap: {e}")
            return None