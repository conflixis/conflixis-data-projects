import { NextResponse } from 'next/server';
import { BigQuery } from '@google-cloud/bigquery';

export async function GET() {
  try {
    const bigquery = new BigQuery({
      projectId: 'data-analytics-389803',
    });
    
    // Query to check the schema
    const query = `
      SELECT 
        column_name,
        data_type
      FROM \`data-analytics-389803.conflixis_datasources.INFORMATION_SCHEMA.COLUMNS\`
      WHERE table_name = 'op_general_all'
        AND column_name LIKE '%indicator%'
      ORDER BY column_name
    `;
    
    const [rows] = await bigquery.query({
      query,
      location: 'us-east4',
    });
    
    return NextResponse.json({
      success: true,
      fields: rows
    });
  } catch (error) {
    return NextResponse.json({
      success: false,
      error: error instanceof Error ? error.message : 'Unknown error'
    });
  }
}