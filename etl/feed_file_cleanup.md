This Python script replicates the logic found in your SQL snapshots—handling the specific string manipulation for PCP names, the conditional mapping for adherence measures, and the deduplication logic.

I’ve structured this to use **Pandas** for the "heavy lifting" of the cleaning before you push the data to the database.

### The Python ETL Script

```python
import pandas as pd
import numpy as np
from datetime import datetime

# --- CONFIGURATION & MAPPINGS ---
# Replicating your CASE WHEN logic for Measures
MEASURE_MAPPING = {
    'ADH-STATINS': 'Statin',
    'ADH-Ace/Arb': 'AceArb',
    'ADH-AceArb': 'AceArb',
    'Statin%': 'Statin',
    '%Ace%Arb%': 'AceArb',
    '%Hypertension%': 'AceArb'
}

def format_pcp_name(name):
    """Replicates your SQL SUBSTRING/POSITION/UPPER logic for PCP names."""
    if pd.isna(name): return None
    # Remove (NPI) suffixes if present
    clean_name = str(name).split('(')[0].strip()
    
    # Split by space to handle 'First Last' -> 'LAST, FIRST'
    parts = clean_name.split(' ')
    if len(parts) >= 2:
        return f"{parts[-1].upper()}, {parts[0].upper()}"
    return clean_name.upper()

def get_managing_entity(group_name):
    """Replicates the CASE logic for RPO and EPIC groups."""
    group_name = str(group_name).upper()
    if 'RPO' in group_name: return 'RENAISSANCE (RPO)'
    if 'EPIC' in group_name: return 'EL PASO INTEGRAL CARE (EPIC)'
    return group_name

# --- FILE PROCESSING FUNCTIONS ---

def process_humana(path):
    # 'skiprows' handles the troublesome header you mentioned
    df = pd.read_excel(path, skiprows=2) 
    
    processed = pd.DataFrame()
    processed['FileName'] = 'MAT_RAW_HUMANA_MA'
    processed['HealthplanName'] = 'Humana'
    processed['PCPName'] = df['Paneled Provider'].apply(format_pcp_name)
    processed['PCPNPI'] = df['Paneled NPI'].astype(str).str.strip()
    processed['MemberName'] = df['Patient Name']
    processed['MemberID'] = df['Humana ID']
    processed['MemberDOB'] = pd.to_datetime(df['Date of Birth'])
    
    # Adherence Mapping
    processed['AdherenceMeasureOrig'] = np.select(
        [df['Measure'] == 'ADH-STATINS', df['Measure'] == 'ADH-Ace/Arb'],
        ['Statin', 'AceArb'], 
        default='Diabetes'
    )
    
    # Replicating line 71: CASE WHEN "In Measure" = 'N' THEN 1 ELSE 2...
    processed['TotalUniqueFillCount'] = np.where(df['In Measure'] == 'N', 1, 2)
    
    return processed

def process_aetna(path, file_label):
    df = pd.read_excel(path)
    
    processed = pd.DataFrame()
    processed['FileName'] = file_label
    processed['HealthplanName'] = 'Aetna'
    processed['HCode'] = df['Contract Number'].str[:5]
    processed['ManagingEntity'] = df['Provider Group Name'].apply(get_managing_entity)
    processed['PCPName'] = df['Provider Name'].apply(format_pcp_name)
    processed['PCPNPI'] = df['NPI'].astype(str).str.strip()
    processed['MemberName'] = df['Member Last Name'] + ', ' + df['Member First Name']
    processed['MemberID'] = df['Member ID']
    processed['MemberDOB'] = pd.to_datetime(df['Member Date of Birth'])
    
    # Filter for Active Members only (Line 122)
    processed = processed[df['Member Status'] == 'Active']
    
    return processed

# --- MAIN EXECUTION ---

# 1. Load and clean all 4 files
# Note: Update these paths to your actual local file locations
humana_df = process_humana('Humana_Feed.xlsx')
aetna_std_df = process_aetna('Aetna_Quality_PartD.xlsx', 'MAT_RAW_AETNA_MA_QUALITYREPORT_PARTD')
aetna_epic_df = process_aetna('Aetna_PartD_Epic.xlsx', 'MAT_RAW_AETNA_MA_QUALITYREPORT_PARTD_EPIC')
aetna_opps_df = process_aetna('Aetna_Opportunities.xlsx', 'MAT_RAW_AETNA_MA_RX_QUALITY_OPPORTUNITIES')

# 2. Union (Concat) everything
final_df = pd.concat([humana_df, aetna_std_df, aetna_epic_df, aetna_opps_df], ignore_index=True)

# 3. Deduplication (Replicating lines 205-215)
# Using your composite key: PCPNPI + MemberID + AdherenceMeasureOrig
final_df = final_df.drop_duplicates(subset=['PCPNPI', 'MemberID', 'AdherenceMeasureOrig'], keep='first')

# 4. Final filter: Last Impactable Date >= Today (Line 201)
# final_df = final_df[final_df['LatestRefillDateForAdherence'] >= datetime.now()]

print(f"Cleaned and Union-ed {len(final_df)} rows. Ready for upload.")

# 5. Upload to Database
# engine = create_engine('teradata://user:pass@host')
# final_df.to_sql('CIN_Multi_Payer_Integration', schema='BISDM_CA_BASE_PRD', con=engine, if_exists='append', index=False)
```

### Key Improvements in this Script:

1.  **The PCP Formatter:** In your SQL, you had to run an `UPDATE` (line 81) after the `INSERT` to fix the name format. In Python, the `format_pcp_name` function does this "on the fly" so the data is perfect before it hits the table.
2.  **Row Filtering:** Instead of loading "Inactive" members and then filtering them out in views later, the `process_aetna` function drops them immediately.
3.  **The "Header Skip":** I included `skiprows=2` in the Humana loader. You can adjust that number based on exactly how many junk rows are at the top of that specific file.
4.  **Managing Entity Logic:** Instead of complex SQL `LIKE` statements, the `get_managing_entity` function uses Python’s cleaner string matching.



Does the "PCP Name" logic (Last, First) need to account for middle initials, or is it always just two parts?
