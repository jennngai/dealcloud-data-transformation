import pandas as pd
import numpy as np
from datetime import datetime
import hashlib
import uuid

class DealCloudTransformer:
    def __init__(self):
        self.audit_trail = []
        self.unique_companies = {}
        self.unique_contacts = {}
        self.choice_fields = {
            'deal_status': set(),
            'sourcing_type': set(),
            'transaction_type': set(),
            'verticals': set(),
            'sub_verticals': set()
        }

    def log_transformation(self, table, action, record_count, notes=""):
        """Maintain audit trail of all transformations"""
        self.audit_trail.append({
            'timestamp': datetime.now().isoformat(),
            'table': table,
            'action': action,
            'record_count': record_count,
            'notes': notes
        })
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {table}: {action} - {record_count} records {notes}")

    def generate_unique_id(self, primary_key, secondary_key=""):
        """Generate consistent unique identifiers"""
        combined_key = f"{primary_key.lower().strip()}_{secondary_key.lower().strip()}"
        return hashlib.md5(combined_key.encode()).hexdigest()[:12]

    def normalize_text(self, text):
        """Standardize text fields for choice field creation"""
        if pd.isna(text) or text == "":
            return None
        return str(text).strip().title()

    def load_pipeline_data(self, filename, header_row):
        """Load pipeline data with proper header detection"""
        try:
            df = pd.read_excel(filename, header=header_row)
            df = df.dropna(how='all').dropna(axis=1, how='all')
            self.log_transformation(filename, "loaded", len(df))
            return df
        except Exception as e:
            self.log_transformation(filename, "ERROR", 0, str(e))
            return pd.DataFrame()

    def extract_companies(self):
        """Extract and remove duplicate companies from all sources"""
        companies = []

        # Load pipeline data
        bs_pipeline = self.load_pipeline_data('Business Services Pipeline.xlsx', 5)
        crh_pipeline = self.load_pipeline_data('Consumer Retail and Healthcare Pipeline.xlsx', 7)

        # Extract companies from Business Services pipeline
        if not bs_pipeline.empty:
            for _, row in bs_pipeline.iterrows():
                company_name = row.get('Company Name', '')
                if pd.notna(company_name) and company_name.strip():
                    company_id = self.generate_unique_id(company_name)

                    if company_id not in self.unique_companies:
                        self.unique_companies[company_id] = {
                            'company_id': company_id,
                            'company_name': self.normalize_text(company_name),
                            'primary_vertical': 'Business Services',
                            'sub_vertical': self.normalize_text(row.get('Sub Vertical', '')),
                            'current_owner': self.normalize_text(row.get('Current Owner', '')),
                            'description': row.get('Business Description', ''),
                            'source_file': 'Business Services Pipeline',
                            'created_date': datetime.now().isoformat()
                        }

        # Extract companies from Consumer Retail Healthcare pipeline
        if not crh_pipeline.empty:
            # Headers might be different - need to map based on actual structure
            for _, row in crh_pipeline.iterrows():
                # Assuming first column is company name based on analysis
                company_name = row.iloc[0] if len(row) > 0 else ''
                if pd.notna(company_name) and company_name.strip():
                    company_id = self.generate_unique_id(company_name)

                    if company_id not in self.unique_companies:
                        vertical = row.iloc[10] if len(row) > 10 else 'Consumer Retail & Healthcare'
                        sub_vertical = row.iloc[11] if len(row) > 11 else ''

                        self.unique_companies[company_id] = {
                            'company_id': company_id,
                            'company_name': self.normalize_text(company_name),
                            'primary_vertical': 'Consumer Retail & Healthcare',
                            'sub_vertical': self.normalize_text(sub_vertical),
                            'current_owner': self.normalize_text(row.iloc[19] if len(row) > 19 else ''),
                            'description': row.iloc[20] if len(row) > 20 else '',
                            'source_file': 'Consumer Retail Healthcare Pipeline',
                            'created_date': datetime.now().isoformat()
                        }