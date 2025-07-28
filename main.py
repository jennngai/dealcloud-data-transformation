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