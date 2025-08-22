// API Types for Disclosure Review System

export interface Disclosure {
  id: string;
  disclosure_id: string;
  campaign_name: string;
  provider_name: string;
  provider_npi: string | null;
  category_label: string;
  relationship_type: string;
  entity_name: string;
  disclosure_type: string | null;
  financial_amount: number | null;
  submission_date: string;
  risk_tier: 'low' | 'moderate' | 'high' | 'critical';
  review_status: 'pending' | 'in_review' | 'approved' | 'requires_management';
  management_plan_required: boolean;
  last_review_date: string;
  next_review_date: string;
  job_title: string | null;
  entity: string | null;
  manager_name: string | null;
  department: string | null;
  notes: string | null;
  open_payments_total: number;
}

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  page_size: number;
  pages: number;
}

export interface PolicyConfig {
  thresholds: {
    tier1_max: number;
    tier2_max: number;
    management_plan_threshold: number;
  };
  risk_categories: {
    [key: string]: {
      weight: number;
      flags: string[];
    };
  };
}

export interface Stats {
  total_disclosures: number;
  unique_providers: number;
  risk_distribution: {
    low: number;
    moderate: number;
    high: number;
    critical: number;
  };
  review_status: {
    pending: number;
    in_review: number;
    approved: number;
    requires_management: number;
  };
  financial_summary: {
    total_amount: number;
    average_amount: number;
    max_amount: number;
    min_amount: number;
  };
}

export interface FilterParams {
  page?: number;
  page_size?: number;
  risk_tier?: string;
  review_status?: string;
  management_plan_required?: boolean;
  search?: string;
  sort_by?: string;
  sort_order?: 'asc' | 'desc';
}

export interface PolicyEvaluation {
  risk_tier: string;
  management_plan_required: boolean;
  flags: string[];
  recommendations: string[];
}