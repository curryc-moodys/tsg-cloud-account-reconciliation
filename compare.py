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

sn_df['account_id'] = sn_df['u_account_reference'].astype(str).str.strip().str.lower()
combined_cloud_df['account_id'] = combined_cloud_df['Account ID'].astype(str).str.strip().str.lower()

sn_df['account_name'] = sn_df['u_account_name'].astype(str).str.strip().str.lower()
combined_cloud_df['account_name'] = combined_cloud_df['Name'].astype(str).str.strip().str.lower()


matches = pd.merge(
    sn_df[['account_id','account_name']],
    combined_cloud_df[['account_id','account_name']],
    on='account_id',
    how='inner',
    suffixes=('_sn', '_pc')
)

#separate matching and differing data
matching_data = matches

differing_data = pd.concat([
    sn_df[['account_id', 'account_name']].rename(columns={'account_name': 'account_name_sn'}),
    combined_cloud_df[['account_id', 'account_name']].rename(columns={'account_name': 'account_name_pc'})
]).drop_duplicates(subset=['account_id'], keep=False)

#save to CSV files
matching_data.to_csv("output/matching_data.csv", index=False)
differing_data.to_csv("output/differing_data.csv", index=False)

