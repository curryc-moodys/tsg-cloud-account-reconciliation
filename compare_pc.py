import pandas as pd

#loading in datasets
#specify encoding to handle potential non-UTF-8 characters
sn_df = pd.read_csv(r"datasets/u_cloud_access_lob_accounts.csv", encoding="latin1")
aws_df = pd.read_csv(r"datasets/cloudAccountProjectsTable-2025-05-06T14_21_50.836Z.csv", encoding="latin1")
azr_df = pd.read_csv(r"datasets/cloudAccountProjectsTable-2025-05-06T19_11_33.793Z.csv", encoding="latin1")
gcp_df = pd.read_csv(r"datasets/cloudAccountProjectsTable-2025-05-06T19_13_47.105Z.csv", encoding="latin1")

#combine aws, azr, and gcp datasets for prisma cloud
cloud_dfs = [aws_df, azr_df, gcp_df]
combined_cloud_df = pd.concat(cloud_dfs, ignore_index=True)

#normalize column names
sn_df.columns = sn_df.columns.str.strip()
combined_cloud_df.columns =  combined_cloud_df.columns.str.strip()

#normalize account_id
sn_df['account_id'] = sn_df['u_account_reference'].astype(str).str.strip().str.lower().str.lstrip('0')
combined_cloud_df['account_id'] = combined_cloud_df['Account ID'].astype(str).str.strip().str.lower().str.lstrip('0')

sn_df['account_name'] = sn_df['u_account_name'].astype(str).str.strip().str.lower().str.replace('-', '_')
combined_cloud_df['account_name'] = combined_cloud_df['Name'].astype(str).str.strip().str.lower().str.replace('-', '_')

# Remove (moody's tenant) from account names
combined_cloud_df['account_name'] = combined_cloud_df['account_name'].str.replace("(moody's tenant)", '', regex=False).str.strip()

# Add cloud provider info to both dataframes
sn_df['cloud_provider'] = sn_df['u_cloud_provider'].str.strip().str.upper()
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
combined_cloud_df['cloud_provider'] = combined_cloud_df['account_name'].apply(infer_cloud_provider)

# Merge on account_id only, keep both account_name columns and cloud_provider for comparison
matches = pd.merge(
    sn_df[['account_id', 'account_name', 'cloud_provider']],
    combined_cloud_df[['account_id', 'account_name', 'cloud_provider']],
    on='account_id',
    how='inner',
    suffixes=('_sn', '_pc')
)

#add fuzzy similarity score for account_name
try:
    from thefuzz import fuzz
    matches['name_similarity'] = matches.apply(
        lambda row: fuzz.ratio(row['account_name_sn'], row['account_name_pc']), axis=1
    )
except ImportError:
    pass  # thefuzz not installed, skip similarity

matching_data = matches

# Find differing data: account_ids that are not present in both
sn_only = sn_df[~sn_df['account_id'].isin(combined_cloud_df['account_id'])][['account_id', 'account_name', 'cloud_provider']]
sn_only = sn_only.rename(columns={'account_name': 'account_name_sn', 'cloud_provider': 'cloud_provider_sn'})
pc_only = combined_cloud_df[~combined_cloud_df['account_id'].isin(sn_df['account_id'])][['account_id', 'account_name', 'cloud_provider']]
pc_only = pc_only.rename(columns={'account_name': 'account_name_pc', 'cloud_provider': 'cloud_provider_pc'})

# For remaining unmatched, try fuzzy name matching
try:
    from thefuzz import process
    # Only consider names not already matched by ID
    sn_names = sn_only['account_name_sn'].tolist()
    pc_names = pc_only['account_name_pc'].tolist()
    fuzzy_matches = []
    for sn_idx, sn_name in enumerate(sn_names):
        match, score = process.extractOne(sn_name, pc_names)
        if score >= 90:  # threshold for strong match
            pc_idx = pc_names.index(match)
            fuzzy_matches.append({
                'account_id': '',
                'account_name_sn': sn_name,
                'cloud_provider_sn': sn_only.iloc[sn_idx]['cloud_provider_sn'],
                'account_name_pc': match,
                'cloud_provider_pc': pc_only.iloc[pc_idx]['cloud_provider_pc'],
                'name_similarity': score
            })
    fuzzy_matches_df = pd.DataFrame(fuzzy_matches)
    # Remove matched names from differing_data
    sn_only = sn_only[~sn_only['account_name_sn'].isin([m['account_name_sn'] for m in fuzzy_matches])]
    pc_only = pc_only[~pc_only['account_name_pc'].isin([m['account_name_pc'] for m in fuzzy_matches])]
    differing_data = pd.concat([sn_only, pc_only], axis=0, ignore_index=True)
    # Add fuzzy matches to matching_data
    matching_data = pd.concat([matching_data, fuzzy_matches_df], ignore_index=True, sort=False)
except ImportError:
    differing_data = pd.concat([sn_only, pc_only], axis=0, ignore_index=True)

#save to CSV files
matching_data.to_csv("output/matching_data_pc.csv", index=False)
differing_data.to_csv("output/differing_data_pc.csv", index=False)

