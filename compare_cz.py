import pandas as pd

#loading in datasets
#specify encoding to handle potential non-UTF-8 characters
sn_df = pd.read_csv(r"datasets/u_cloud_access_lob_accounts.csv", encoding="latin1")

#combine aws, azr, and gcp datasets for prisma cloud
combined_cloud_df = pd.read_csv(r"datasets\Account _ Account Name.csv", encoding="latin1")

#normalize column names
sn_df.columns = sn_df.columns.str.strip()
combined_cloud_df.columns =  combined_cloud_df.columns.str.strip()

# Normalize account_id by removing leading zeros
sn_df['account_id'] = sn_df['u_account_reference'].astype(str).str.strip().str.lower().str.lstrip('0')
combined_cloud_df['account_id'] = combined_cloud_df['Cloud Provider Dimensions Account'].astype(str).str.strip().str.lower().str.lstrip('0')

# Normalize account_name by replacing '-' with '_' for both dataframes
sn_df['account_name'] = sn_df['u_account_name'].astype(str).str.strip().str.lower().str.replace('-', '_')
combined_cloud_df['account_name'] = combined_cloud_df['Custom Dimensions Account Name'].astype(str).str.strip().str.lower().str.replace('-', '_')

# Remove (moody's tenant) and account_id from account names, only if present
sn_df['account_name'] = sn_df.apply(
    lambda row: row['account_name'].replace("(moody's tenant)", '').replace(row['account_id'], '').strip() if row['account_id'] and row['account_id'] in row['account_name'] else row['account_name'].replace("(moody's tenant)", '').strip(), axis=1)
combined_cloud_df['account_name'] = combined_cloud_df.apply(
    lambda row: row['account_name'].replace("(moody's tenant)", '').replace(row['account_id'], '').strip() if row['account_id'] and row['account_id'] in row['account_name'] else row['account_name'].replace("(moody's tenant)", '').strip(), axis=1)

# Remove empty account names after cleaning
sn_df = sn_df[sn_df['account_name'] != '']
combined_cloud_df = combined_cloud_df[combined_cloud_df['account_name'] != '']

# Add cloud provider info to both dataframes
sn_df['cloud_provider'] = sn_df['u_cloud_provider'].str.strip().str.upper()
combined_cloud_df['cloud_provider'] = combined_cloud_df['Cloud Provider Dimensions Cloud Provider'].str.strip().str.upper()

# Merge on account_id only, keep both account_name columns and cloud_provider for comparison
matches = pd.merge(
    sn_df[['account_id', 'account_name', 'cloud_provider']],
    combined_cloud_df[['account_id', 'account_name', 'cloud_provider']],
    on='account_id',
    how='inner',
    suffixes=('_sn', '_cz')
)

#add fuzzy similarity score for account_name
try:
    from thefuzz import fuzz
    matches['name_similarity'] = matches.apply(
        lambda row: fuzz.ratio(row['account_name_sn'], row['account_name_cz']), axis=1
    )
except ImportError:
    pass  # thefuzz not installed, skip similarity

matching_data = matches

# Find differing data: account_ids that are not present in both
sn_only = sn_df[~sn_df['account_id'].isin(combined_cloud_df['account_id'])][['account_id', 'account_name', 'cloud_provider']]
sn_only = sn_only.rename(columns={'account_name': 'account_name_sn', 'cloud_provider': 'cloud_provider_sn'})
cz_only = combined_cloud_df[~combined_cloud_df['account_id'].isin(sn_df['account_id'])][['account_id', 'account_name', 'cloud_provider']]
cz_only = cz_only.rename(columns={'account_name': 'account_name_cz', 'cloud_provider': 'cloud_provider_cz'})

# For remaining unmatched, try fuzzy name matching
try:
    from thefuzz import process
    sn_names = sn_only['account_name_sn'].tolist()
    cz_names = cz_only['account_name_cz'].tolist()
    fuzzy_matches = []
    for sn_idx, sn_name in enumerate(sn_names):
        if not sn_name:
            continue
        match, score = process.extractOne(sn_name, cz_names)
        if match and score >= 90:
            cz_idx = cz_names.index(match)
            fuzzy_matches.append({
                'account_id': '',
                'account_name_sn': sn_name,
                'cloud_provider_sn': sn_only.iloc[sn_idx]['cloud_provider_sn'],
                'account_name_cz': match,
                'cloud_provider_cz': cz_only.iloc[cz_idx]['cloud_provider_cz'],
                'name_similarity': score
            })
    fuzzy_matches_df = pd.DataFrame(fuzzy_matches)
    sn_only = sn_only[~sn_only['account_name_sn'].isin([m['account_name_sn'] for m in fuzzy_matches])]
    cz_only = cz_only[~cz_only['account_name_cz'].isin([m['account_name_cz'] for m in fuzzy_matches])]
    differing_data = pd.concat([sn_only, cz_only], axis=0, ignore_index=True)
    matching_data = pd.concat([matching_data, fuzzy_matches_df], ignore_index=True, sort=False)
except ImportError:
    differing_data = pd.concat([sn_only, cz_only], axis=0, ignore_index=True)

#save to CSV files
matching_data.to_csv("output/matching_data_cz.csv", index=False)
differing_data.to_csv("output/differing_data_cz.csv", index=False)

