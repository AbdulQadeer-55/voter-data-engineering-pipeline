import pandas as pd
import numpy as np
import re

def run_voter_pipeline(input_file):
    # Load data with strict string types to preserve leading zeros in zip codes
    # Source layout defines 47 columns
    df = pd.read_csv(input_file, dtype=str, quotechar='"', skipinitialspace=True, header=None)
    
    # Map key columns based on FOIL_VOTER_LIST_LAYOUT
    # Column indices are zero-based (Field Position - 1)
    cols = {
        'LASTNAME': 0, 'FIRSTNAME': 1, 'MIDDLENAME': 2, 'NAMESUFFIX': 3,
        'RADDNUMBER': 4, 'RHALFCODE': 5, 'RPREDIRECTION': 6, 'RSTREETNAME': 7,
        'RCITY': 12, 'RZIP5': 13, 'STATUS': 41, 'REASONCODE': 42, 
        'PURGE_DATE': 44, 'SBOEID': 45, 'HISTORY': 46, 'ENROLLMENT': 21,
        'ED': 24, 'AD': 30, 'REGDATE': 37, 'VRSOURCE': 38, 'IDREQ': 39
    }
    
    # 1. Status Filtering: Keep only Active 'A' and exclude Purged/Inactive dates
    df = df[df[cols['STATUS']] == 'A'].copy()
    df = df[df[cols['PURGE_DATE']].isna()].copy()
    
    # 2. Address Normalization
    df['RADDNUMBER_FULL'] = df[cols['RADDNUMBER']].fillna('') + " " + df[cols['RHALFCODE']].fillna('')
    df['RADDNUMBER_FULL'] = df['RADDNUMBER_FULL'].str.strip()
    
    df['RSTREET_FULL'] = df[cols['RPREDIRECTION']].fillna('') + " " + df[cols['RSTREETNAME']].fillna('')
    df['RSTREET_FULL'] = df['RSTREET_FULL'].str.strip()
    
    df['FULL_ADDRESS'] = (
        df['RADDNUMBER_FULL'] + " " + 
        df['RSTREET_FULL'] + ", " + 
        df[cols['RCITY']].fillna('') + ", NY " + 
        df[cols['RZIP5']].fillna('').str.zfill(5)
    )
    
    # 3. Voter History Parsing (June Primaries)
    def parse_history(history_str):
        if pd.isna(history_str):
            return pd.Series([None, 0, 0, False, 0])
        
        # Find all 8-digit dates followed by election codes
        dates = re.findall(r'(\d{8})', str(history_str))
        june_dates = [d for d in dates if d[4:6] == '06']
        
        all_time_count = len(june_dates)
        since_2020_count = len([d for d in june_dates if int(d[:4]) >= 2020])
        loyalty_6yr_count = len([d for d in june_dates if int(d[:4]) in [2018, 2020, 2022, 2024]])
        
        last_june = max(june_dates) if june_dates else None
        flag = since_2020_count >= 2
        
        return pd.Series([last_june, all_time_count, since_2020_count, flag, loyalty_6yr_count])

    history_cols = ['LAST_JUNE_ELECTION_DATE', 'JUNE_PRIMARY_COUNT_ALL_TIME', 
                    'JUNE_PRIMARY_COUNT_SINCE_2020', 'LIKELY_PRIMARY_VOTER_FLAG', 'LOYALTY_SCORE_6YR']
    
    df[history_cols] = df[cols['HISTORY']].apply(parse_history)
    
    # 4. Geocoding Prep
    df['LAT'] = np.nan
    df['LON'] = np.nan
    
    # Separate Ungeocodeable (Missing house number or street)
    mask_ungeo = (df[cols['RADDNUMBER']].isna()) | (df[cols['RSTREETNAME']].isna())
    ungeocodable_df = df[mask_ungeo].copy()
    clean_df = df[~mask_ungeo].copy()
    
    # 5. Final Column Selection & Ordering
    final_cols = [
        cols['SBOEID'], cols['LASTNAME'], cols['FIRSTNAME'], cols['MIDDLENAME'], cols['NAMESUFFIX'],
        cols['ENROLLMENT'], cols['STATUS'], cols['ED'], cols['AD'], 'FULL_ADDRESS'
    ] + history_cols + ['LAT', 'LON', cols['REGDATE'], cols['VRSOURCE'], cols['IDREQ'], cols['REASONCODE']]
    
    # Rename for readability
    rename_map = {
        cols['SBOEID']: 'SBOEID', cols['LASTNAME']: 'LASTNAME', cols['FIRSTNAME']: 'FIRSTNAME',
        cols['MIDDLENAME']: 'MIDDLENAME', cols['NAMESUFFIX']: 'NAMESUFFIX',
        cols['ENROLLMENT']: 'PARTY', cols['STATUS']: 'STATUS', cols['ED']: 'ED', 
        cols['AD']: 'AD', cols['REGDATE']: 'REG_DATE', cols['VRSOURCE']: 'REG_SOURCE',
        cols['IDREQ']: 'ID_REQUIRED', cols['REASONCODE']: 'REASON_CODE'
    }
    
    final_master = clean_df[final_cols].rename(columns=rename_map)
    
    # 6. Deliverable Exports
    final_master.to_excel("Master_Voter_File.xlsx", index=False)
    final_master.to_parquet("Master_Voter_File.parquet", index=False)
    
    geocoding_csv = final_master[['SBOEID', 'FULL_ADDRESS']]
    geocoding_csv.to_csv("Geocoding_Export.csv", index=False)
    
    ungeocodable_df.to_excel("Ungeocodeable_Records.xlsx", index=False)
    
    print("Project processing complete. Files generated.")

run_voter_pipeline("data/Untitled.txt")