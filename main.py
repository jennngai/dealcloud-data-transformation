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

        # Extract PE competitor companies
        try:
            pe_comps = pd.read_excel('PE Comps.xlsx', header=1)
            pe_comps = pe_comps.dropna(how='all')

            for _, row in pe_comps.iterrows():
                company_name = row.iloc[1] if len(row) > 1 else ''  # Company Name column
                if pd.notna(company_name) and company_name.strip():
                    company_id = self.generate_unique_id(company_name)

                    if company_id not in self.unique_companies:
                        self.unique_companies[company_id] = {
                            'company_id': company_id,
                            'company_name': self.normalize_text(company_name),
                            'company_type': 'Private Equity Firm',
                            'website': row.iloc[2] if len(row) > 2 else '',
                            'aum_billions': row.iloc[3] if len(row) > 3 else None,
                            'sectors': row.iloc[4] if len(row) > 4 else '',
                            'portfolio_companies': row.iloc[5] if len(row) > 5 else '',
                            'source_file': 'PE Comps',
                            'created_date': datetime.now().isoformat()
                        }
        except Exception as e:
            self.log_transformation("PE Comps", "ERROR", 0, str(e))

        companies_df = pd.DataFrame(list(self.unique_companies.values()))
        self.log_transformation("Companies", "extracted", len(companies_df))
        return companies_df

    def extract_contacts(self):
        """Extract and merge contact data from multiple sources"""
        contacts = []

        # Load contact data from main contacts file
        try:
            # Tier 1 contacts
            tier1_contacts = pd.read_excel('Contacts.xlsx', sheet_name="Tier 1's")
            for _, row in tier1_contacts.iterrows():
                contact_id = self.generate_unique_id(row['Name'], row['Firm'])

                self.unique_contacts[contact_id] = {
                    'contact_id': contact_id,
                    'name': row['Name'],
                    'email': row['E-mail'],
                    'firm': row['Firm'],
                    'title': row['Title'],
                    'phone': row['Phone'],
                    'city': row['City'],
                    'birthday': row['Birthday'],
                    'group': row['Group'],
                    'sub_vertical': row['Sub-Vertical'],
                    'coverage_person': row['Coverage Person'],
                    'preferred_contact_method': row['Preferred Contact Method'],
                    'tier': 'Tier 1',
                    'source_file': 'Contacts - Tier 1',
                    'created_date': datetime.now().isoformat()
                }

            # Tier 2 contacts
            tier2_contacts = pd.read_excel('Contacts.xlsx', sheet_name="Tier 2's")
            for _, row in tier2_contacts.iterrows():
                contact_id = self.generate_unique_id(row['Name'], row['Firm'])

                if contact_id not in self.unique_contacts:  # Avoid duplicates
                    self.unique_contacts[contact_id] = {
                        'contact_id': contact_id,
                        'name': row['Name'],
                        'email': row['E-mail'],
                        'firm': row['Firm'],
                        'title': row['Title'],
                        'phone': row['Phone'],
                        'city': row['City'],
                        'birthday': row['Birthday'],
                        'group': row['Group'],
                        'sub_vertical': row['Sub-Vertical'],
                        'coverage_person': row['Coverage Person'],
                        'preferred_contact_method': row['Preferred Contact Method'],
                        'tier': 'Tier 2',
                        'source_file': 'Contacts - Tier 2',
                        'created_date': datetime.now().isoformat()
                    }

        except Exception as e:
            self.log_transformation("Contacts", "ERROR", 0, str(e))

        # Add event attendees as contacts
        try:
            events_file = pd.ExcelFile('Events.xlsx')
            for sheet_name in events_file.sheet_names:
                event_attendees = pd.read_excel('Events.xlsx', sheet_name=sheet_name)

                for _, row in event_attendees.iterrows():
                    # Use email as primary identifier for event contacts
                    contact_id = self.generate_unique_id(row['E-mail'], row['Name'])

                    if contact_id not in self.unique_contacts:
                        # Extract firm from email domain
                        email_domain = row['E-mail'].split('@')[-1] if '@' in str(row['E-mail']) else ''
                        firm_from_email = email_domain.replace('.com', '').replace('.', ' ').title()

                        self.unique_contacts[contact_id] = {
                            'contact_id': contact_id,
                            'name': row['Name'],
                            'email': row['E-mail'],
                            'firm': firm_from_email,
                            'attendee_status': row['Attendee Status'],
                            'last_event_attended': sheet_name,
                            'source_file': f'Events - {sheet_name}',
                            'created_date': datetime.now().isoformat()
                        }

        except Exception as e:
            self.log_transformation("Event Contacts", "ERROR", 0, str(e))

        contacts_df = pd.DataFrame(list(self.unique_contacts.values()))
        self.log_transformation("Contacts", "extracted", len(contacts_df))
        return contacts_df