# DealCloud Data Transformation
## Overview
This project transforms a Private Equity firm's Excel data files into normalized CSV format for import into DealCloud. The transformation handles pipeline data from two verticals (Business Services and Consumer Retail & Healthcare), contact management data, event attendee information, and PE competitor intelligence.

## Quick Start
Prerequisites
```bash
pip install -r requirements.txt
```

## File Setup
Place the following Excel files in the project directory:

- Business Services Pipeline.xlsx
- Consumer Retail and Healthcare Pipeline.xlsx
- Contacts.xlsx
- Events.xlsx
- PE Comps.xlsx

## Run Transformation

```bash
python main.py
```

## Expected Output Files

The script generates 6 CSV files ready for DealCloud import:

| File | Description | Import Order |
|------|-------------|--------------|
| `dealcloud_companies.csv` | Unique companies from all sources | 1st |
| `dealcloud_contacts.csv` | Merged contact data with tier classification | 2nd |
| `dealcloud_deals.csv` | Combined pipeline deals from both verticals | 3rd |
| `dealcloud_marketing_participants.csv` | Event attendee data | 4th |
| `dealcloud_choice_fields.csv` | Standardized dropdown options | (Configuration) |
| `transformation_audit_trail.csv` | Complete processing log | (Reference) |

## Key Features

- Unique ID Generation: Creates consistent identifiers to handle duplicate names
- Data Normalization: Standardizes text fields for clean dropdown options
- Audit Trail: Comprehensive logging for debugging and validation
- Choice Field Extraction: Identifies all unique values for DealCloud configuration
- Relationship Mapping: Maintains data relationships through foreign keys