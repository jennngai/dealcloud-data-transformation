# DealCloud Data Transformation
## Overview
This project transforms a Private Equity firm's Excel data files into normalized CSV format for import into DealCloud. The transformation handles pipeline data from two verticals (Business Services and Consumer Retail & Healthcare), contact management data, event attendee information, and PE competitor intelligence.

## Quick Start
Prerequisites
```bash
bashpip install pandas openpyxl numpy
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
bashpython main.py
```

## Expected Output Files

The script generates 6 CSV files ready for DealCloud import: