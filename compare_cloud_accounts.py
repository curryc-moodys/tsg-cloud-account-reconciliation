import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz, process
import json
import re
import argparse
import glob
import os

# =============================================================================
# NORMALIZATION FUNCTIONS
# =============================================================================
def normalize_account_id(series):
    """
    Normalize account_id: strip, lower, remove leading zeros
    """
    return series.astype(str).str.strip().str.lower().str.lstrip('0')

def normalize_account_name(series):
    """
    Normalize account_name: strip, lower, replace hyphens with underscores, remove spaces, remove leading zeros,
    remove (moody's tenant) (case-insensitive, with or without punctuation/whitespace), 
    remove periods, remove special chars except underscores
    """
    cleaned = (
        series.astype(str)
        .str.strip()
        .str.lower()
        .str.replace('-', '_')
        .str.replace(' ', '')
        .str.lstrip('0')
        .str.replace(r"\(moody['’']?s tenant\)", '', regex=True, case=False)
        .str.replace('.', '', regex=False)
    )
    # Remove all non-alphanumeric and non-underscore characters
    cleaned = cleaned.apply(lambda x: re.sub(r'[^a-z0-9_]', '', x))
    return cleaned

def infer_cloud_provider(name):
    name = str(name).lower().replace('-', '_')
    if 'aws' in name:
        return 'AWS'
    elif 'azr' in name:
        return 'AZURE'
    elif 'gcp' in name:
        return 'GCP'
    else:
        return ''

# =============================================================================
# LOGGING UTILITY
# =============================================================================
def log_and_print(log_lines, message):
    print(message)
    log_lines.append(message + '\n')

# =============================================================================
# MAIN RECONCILIATION LOGIC
# =============================================================================
def main():


    parser = argparse.ArgumentParser(description='CMDB vs Prisma Cloud Reconciliation')
    parser.add_argument('--sn-dir', type=str, default='datasets/SN/', help='Directory for ServiceNow (CMDB) CSVs')
    parser.add_argument('--pc-dir', type=str, default='datasets/PC/', help='Directory for Prisma Cloud CSVs')
    parser.add_argument('--fuzzy-threshold', type=int, default=80, help='Fuzzy match threshold (0-100)')
    parser.add_argument('--output-prefix', type=str, default='output/', help='Output directory prefix')
    args = parser.parse_args()

    log_lines = []

    # Load all ServiceNow CSVs (if you expect only one, just use sn_files[0])
    sn_files = glob.glob(os.path.join(args.sn_dir, '*.csv'))
    if not sn_files:
        raise FileNotFoundError(f"No ServiceNow CSVs found in {args.sn_dir}")
    log_and_print(log_lines, f"Loading ServiceNow data from: {sn_files}")
    sn_dfs = [pd.read_csv(f, encoding='latin1') for f in sn_files]
    sn_df = pd.concat(sn_dfs, ignore_index=True) if len(sn_dfs) > 1 else sn_dfs[0]
    log_and_print(log_lines, f"Loaded {len(sn_df)} ServiceNow records")

    # Load all Prisma Cloud CSVs
    pc_files = glob.glob(os.path.join(args.pc_dir, '*.csv'))
    if not pc_files:
        raise FileNotFoundError(f"No Prisma Cloud CSVs found in {args.pc_dir}")
    log_and_print(log_lines, f"Loading Prisma Cloud data from: {pc_files}")
    pc_dfs = [pd.read_csv(f, encoding='latin1') for f in pc_files]
    combined_cloud_df = pd.concat(pc_dfs, ignore_index=True)
    log_and_print(log_lines, f"Loaded {len(combined_cloud_df)} Prisma Cloud records")

    # Normalize column names
    sn_df.columns = sn_df.columns.str.strip()
    combined_cloud_df.columns = combined_cloud_df.columns.str.strip()

    # Normalize account_id and account_name
    # Use 'u_short_account_number' as the ServiceNow account_id column (updated for new dataset)
    sn_df['account_id'] = normalize_account_id(sn_df['u_short_account_number'])
    combined_cloud_df['account_id'] = normalize_account_id(combined_cloud_df['Account ID'])
    sn_df['account_name'] = normalize_account_name(sn_df['u_account_name'])
    combined_cloud_df['account_name'] = normalize_account_name(combined_cloud_df['Name'])

    # Add cloud provider info
    sn_df['cloud_provider'] = sn_df['u_cloud_provider'].astype(str).str.strip().str.upper()
    combined_cloud_df['cloud_provider'] = combined_cloud_df['account_name'].apply(infer_cloud_provider)

    # Do NOT remove duplicates
    # sn_df = sn_df.drop_duplicates(subset=['account_id', 'account_name'])
    # combined_cloud_df = combined_cloud_df.drop_duplicates(subset=['account_id', 'account_name'])
    log_and_print(log_lines, f"Duplicates retained. SN: {len(sn_df)} records, Prisma: {len(combined_cloud_df)} records")

    # Merge on account_id (all matches, including duplicates)
    matches = pd.merge(
        sn_df[['account_id', 'account_name', 'cloud_provider']],
        combined_cloud_df[['account_id', 'account_name', 'cloud_provider']],
        on='account_id',
        how='inner',
        suffixes=('_sn', '_pc')
    )
    log_and_print(log_lines, f"Found {len(matches)} direct account_id matches (duplicates retained)")

    # Find differences (records not matching by account_id)
    sn_only = sn_df[~sn_df['account_id'].isin(combined_cloud_df['account_id'])]
    pc_only = combined_cloud_df[~combined_cloud_df['account_id'].isin(sn_df['account_id'])]
    log_and_print(log_lines, f"Found {len(sn_only)} CMDB-only records and {len(pc_only)} Prisma-only records (by account_id)")

    # Fuzzy match for unmatched (optional, can be commented out if not needed)
    fuzzy_matches = []
    for sn_idx, sn_row in sn_only.iterrows():
        if pc_only['account_name'].empty:
            continue
        result = process.extractOne(sn_row['account_name'], pc_only['account_name'])
        if result is not None:
            match, score = result[0], result[1]
            if score >= args.fuzzy_threshold:
                pc_match = pc_only[pc_only['account_name'] == match]
                if not pc_match.empty:
                    pc_match = pc_match.iloc[0]
                    fuzzy_matches.append({
                        'account_id': '',
                        'account_name_sn': sn_row['account_name'],
                        'cloud_provider_sn': sn_row['cloud_provider'],
                        'account_name_pc': pc_match['account_name'],
                        'cloud_provider_pc': pc_match['cloud_provider'],
                        'name_similarity': score
                    })
    fuzzy_matches_df = pd.DataFrame(fuzzy_matches)
    log_and_print(log_lines, f"Found {len(fuzzy_matches_df)} fuzzy name matches (score >= {args.fuzzy_threshold})")

    # Ensure normalization is applied to account_name columns in all outputs
    # Apply normalization to both account_name and account_id columns in all outputs
    def normalize_output(df):
        for col in df.columns:
            if 'account_name' in col:
                df[col] = normalize_account_name(df[col])
            if 'account_id' in col:
                df[col] = normalize_account_id(df[col])
        return df

    matches = normalize_output(matches)
    sn_only = normalize_output(sn_only)
    pc_only = normalize_output(pc_only)
    fuzzy_matches_df = normalize_output(fuzzy_matches_df)
    differences = normalize_output(pd.concat([sn_only, pc_only], axis=0, ignore_index=True))

    matches.to_csv(f'{args.output_prefix}matching_data_pc_new.csv', index=False)
    sn_only.to_csv(f'{args.output_prefix}cmdb_only_pc.csv', index=False)
    pc_only.to_csv(f'{args.output_prefix}prisma_only_pc.csv', index=False)
    fuzzy_matches_df.to_csv(f'{args.output_prefix}fuzzy_matches_pc.csv', index=False)
    differences.to_csv(f'{args.output_prefix}differences_pc.csv', index=False)
    log_and_print(log_lines, f"Saved output files to {args.output_prefix} (including differences_pc.csv, all normalized)")

    # Output summary statistics
    summary = {
        'total_cmdb_records': len(sn_df),
        'total_prisma_records': len(combined_cloud_df),
        'direct_matches': len(matches),
        'cmdb_only': len(sn_only),
        'prisma_only': len(pc_only),
        'fuzzy_matches': len(fuzzy_matches_df),
        'fuzzy_threshold': args.fuzzy_threshold
    }
    with open(f'{args.output_prefix}summary_pc.json', 'w') as f:
        json.dump(summary, f, indent=2)
    log_and_print(log_lines, f"Summary statistics saved to {args.output_prefix}summary_pc.json")

    with open(f'{args.output_prefix}compare_pc_new_log.txt', 'w') as f:
        f.writelines(log_lines)
    print(f"Log written to {args.output_prefix}compare_pc_new_log.txt")

if __name__ == "__main__":
    main()

# This script is designed to compare ServiceNow CMDB data with Prisma Cloud data