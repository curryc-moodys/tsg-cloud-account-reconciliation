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

#normalize account_id by removing leading zeros
sn_df['account_id'] = sn_df['u_account_reference'].astype(str).str.strip().str.lower().str.lstrip('0')
combined_cloud_df['account_id'] = combined_cloud_df['Account ID'].astype(str).str.strip().str.lower().str.lstrip('0')

sn_df['account_name'] = sn_df['u_account_name'].astype(str).str.strip().str.lower()
combined_cloud_df['account_name'] = combined_cloud_df['Name'].astype(str).str.strip().str.lower()

matches = pd.merge(
    sn_df[['account_id', 'account_name']],
    combined_cloud_df[['account_id', 'account_name']],
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

#differing data: account_ids that are not present in both
sn_only = sn_df[~sn_df['account_id'].isin(combined_cloud_df['account_id'])][['account_id', 'account_name']]
sn_only = sn_only.rename(columns={'account_name': 'account_name_sn'})
pc_only = combined_cloud_df[~combined_cloud_df['account_id'].isin(sn_df['account_id'])][['account_id', 'account_name']]
pc_only = pc_only.rename(columns={'account_name': 'account_name_pc'})
differing_data = pd.concat([sn_only, pc_only], axis=0, ignore_index=True)

#save to CSV files
matching_data.to_csv("output/matching_data.csv", index=False)
differing_data.to_csv("output/differing_data.csv", index=False)

