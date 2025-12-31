import pandas as pd
import numpy as np
import re

def run_voter_pipeline(input_file):
    df = pd.read_csv(input_file, dtype=str, quotechar="\"", skipinitialspace=True, header=None)
    
    cols = {
        "LASTNAME": 0, "FIRSTNAME": 1, "MIDDLENAME": 2, "NAMESUFFIX": 3,
        "RADDNUMBER": 4, "RHALFCODE": 5, "RPREDIRECTION": 6, "RSTREETNAME": 7,
        "RAPARTMENT": 9, "RADDL_ADDRESS": 10,
        "RCITY": 12, "RZIP5": 13, "STATUS": 41, "REASONCODE": 42, 
        "PURGE_DATE": 44, "SBOEID": 45, "HISTORY": 46, "ENROLLMENT": 21,
        "ED": 24, "AD": 30, "REGDATE": 37, "VRSOURCE": 38, "IDREQ": 39
    }
    
    df = df[df[cols["STATUS"]] == "A"].copy()
    df = df[df[cols["PURGE_DATE"]].isna()].copy()
    
    df["RADDNUMBER_FULL"] = df[cols["RADDNUMBER"]].fillna("") + " " + df[cols["RHALFCODE"]].fillna("")
    df["RADDNUMBER_FULL"] = df["RADDNUMBER_FULL"].str.strip()
    
    df["RSTREET_FULL"] = df[cols["RPREDIRECTION"]].fillna("") + " " + df[cols["RSTREETNAME"]].fillna("")
    df["RSTREET_FULL"] = df["RSTREET_FULL"].str.strip()
    
    df["UNIT_FULL"] = df[cols["RAPARTMENT"]].fillna("") + " " + df[cols["RADDL_ADDRESS"]].fillna("")
    df["UNIT_FULL"] = df["UNIT_FULL"].str.strip()
    
    df["FULL_ADDRESS"] = (
        df["RADDNUMBER_FULL"] + " " + 
        df["RSTREET_FULL"] + " " + 
        df["UNIT_FULL"] + ", " + 
        df[cols["RCITY"]].fillna("") + ", NY " + 
        df[cols["RZIP5"]].fillna("").str.zfill(5)
    )
    
    def parse_history(history_str):
        if pd.isna(history_str):
            return pd.Series([None, 0, 0, False, 0])
        
        dates = re.findall(r"(\d{8})", str(history_str))
        june_dates = [d for d in dates if d[4:6] == "06"]
        
        all_time_count = len(june_dates)
        since_2020_count = len([d for d in june_dates if int(d[:4]) >= 2020])
        loyalty_6yr_count = len([d for d in june_dates if int(d[:4]) in [2018, 2020, 2022, 2024]])
        
        last_june = max(june_dates) if june_dates else None
        flag = since_2020_count >= 2
        
        return pd.Series([last_june, all_time_count, since_2020_count, flag, loyalty_6yr_count])

    history_cols = ["LAST_JUNE_ELECTION_DATE", "JUNE_PRIMARY_COUNT_ALL_TIME", 
                    "JUNE_PRIMARY_COUNT_SINCE_2020", "LIKELY_PRIMARY_VOTER_FLAG", "LOYALTY_SCORE_6YR"]
    
    df[history_cols] = df[cols["HISTORY"]].apply(parse_history)
    
    df["LAT"] = np.nan
    df["LON"] = np.nan
    
    mask_ungeo = (df[cols["RADDNUMBER"]].isna()) | (df[cols["RSTREETNAME"]].isna())
    ungeocodable_df = df[mask_ungeo].copy()
    clean_df = df[~mask_ungeo].copy()
    
    final_cols = [
        cols["SBOEID"], cols["LASTNAME"], cols["FIRSTNAME"], cols["MIDDLENAME"], cols["NAMESUFFIX"],
        cols["RADDNUMBER"], cols["RHALFCODE"], cols["RPREDIRECTION"], cols["RSTREETNAME"],
        cols["RAPARTMENT"], cols["RADDL_ADDRESS"], "UNIT_FULL",
        cols["RCITY"], cols["RZIP5"], "FULL_ADDRESS",
        cols["ENROLLMENT"], cols["STATUS"], cols["ED"], cols["AD"]
    ] + history_cols + ["LAT", "LON", cols["REGDATE"], cols["VRSOURCE"], cols["IDREQ"], cols["REASONCODE"]]
    
    rename_map = {
        cols["SBOEID"]: "SBOEID", cols["LASTNAME"]: "LASTNAME", cols["FIRSTNAME"]: "FIRSTNAME",
        cols["MIDDLENAME"]: "MIDDLENAME", cols["NAMESUFFIX"]: "NAMESUFFIX",
        cols["RADDNUMBER"]: "HOUSE_NUMBER", cols["RHALFCODE"]: "FRACTION",
        cols["RPREDIRECTION"]: "DIRECTION", cols["RSTREETNAME"]: "STREET_NAME",
        cols["RAPARTMENT"]: "APARTMENT", cols["RADDL_ADDRESS"]: "ADDL_ADDRESS",
        "UNIT_FULL": "UNIT",
        cols["RCITY"]: "CITY", cols["RZIP5"]: "ZIP_CODE",
        cols["ENROLLMENT"]: "PARTY", cols["STATUS"]: "STATUS", cols["ED"]: "ED", 
        cols["AD"]: "AD", cols["REGDATE"]: "REG_DATE", cols["VRSOURCE"]: "REG_SOURCE",
        cols["IDREQ"]: "ID_REQUIRED", cols["REASONCODE"]: "REASON_CODE"
    }
    
    final_master = clean_df[final_cols].rename(columns=rename_map)
    
    final_master.to_excel("Master_Voter_File.xlsx", index=False)
    final_master.to_parquet("Master_Voter_File.parquet", index=False)
    
    geocoding_csv = final_master[["SBOEID", "FULL_ADDRESS"]]
    geocoding_csv.to_csv("Geocoding_Export.csv", index=False)
    
    ungeocodable_df.to_excel("Ungeocodeable_Records.xlsx", index=False)
    
    print("Project processing complete. Files generated.")

run_voter_pipeline("data/Untitled.txt")