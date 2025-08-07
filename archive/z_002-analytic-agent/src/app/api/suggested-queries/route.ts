import { NextResponse } from 'next/server';
import type { SuggestedQuery } from '@/types';

export async function GET() {
  const suggestions: SuggestedQuery[] = [
    {
      title: "High-Risk Speaker Fees",
      description: "Identify physicians with unusually high speaker fee patterns",
      query: "Show me physicians who received speaker fees from more than 5 different companies last year",
      complianceFocus: "Multiple speaker arrangements can indicate potential kickback schemes"
    },
    {
      title: "Ownership & Payments Analysis",
      description: "Find physicians with ownership interests receiving high payments",
      query: "Which physicians with ownership interests received over $100,000 in payments?",
      complianceFocus: "Ownership combined with high payments presents significant conflict of interest risks"
    },
    {
      title: "Geographic Hotspots",
      description: "Identify cities with unusually high payment concentrations",
      query: "What are the top 10 cities by average consulting fee payments to specialists?",
      complianceFocus: "Geographic concentrations may indicate localized inappropriate relationships"
    },
    {
      title: "Year-over-Year Increases",
      description: "Find physicians with sudden payment increases",
      query: "Show me physicians whose payments increased by more than 300% this year",
      complianceFocus: "Sudden payment spikes often correlate with new inappropriate arrangements"
    },
    {
      title: "Travel & Entertainment Risk",
      description: "Analyze luxury travel patterns",
      query: "Which physicians received travel payments to international destinations?",
      complianceFocus: "Luxury travel is a common vehicle for inappropriate inducements"
    },
    {
      title: "Competitive Drug Payments",
      description: "Physicians receiving from multiple competitors",
      query: "Find cardiologists receiving payments from multiple statin manufacturers",
      complianceFocus: "Payments from competitors for same drug class raise neutrality concerns"
    }
  ];

  return NextResponse.json(suggestions);
}