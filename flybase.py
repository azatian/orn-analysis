# %%
from sqlalchemy import create_engine, text
import pandas as pd
# %%
np = pd.read_csv("inputs/neuropeptides_dump_5_2_23_ya.csv")
# %%
np["Short Name"].str.strip().tolist()
# %%
short_names = tuple(np["Short Name"].str.strip().tolist())
# %%
#Accessing CHADO and searching for Neuropeptide names and extracting their Gene IDs
engine = create_engine('postgresql+psycopg2://flybase@chado.flybase.org/flybase')
# %%
connection = engine.connect()
# %%
sqlstring = '''select * from feature where feature.name in ''' + str(short_names) + ''' and feature.is_obsolete=False'''
# %%
sqlstring
# %%
df1 = pd.read_sql(text(sqlstring), connection)
# %%
returned_nps = df1["name"].tolist()
# %%
missing_nps = set(short_names).difference(set(returned_nps))
# %%
#the following is expected since AstB is actually Mip
missing_nps
# %%
tbl = dict(zip(df1['name'], df1['uniquename']))
# %%
tbl
# %%
#From here on out use the FlyBase API for querying
import requests
import time
api_url = 'https://api.flybase.org/api/v1.0/gene/summaries/auto/'

for_df = []
for key, value in tbl.items():
    temp  = []
    temp.append(key)
    temp.append(value)
    response = requests.get(api_url+value)
    try:
        summary = response.json()["resultset"]['result'][0]['summary']
        temp.append(summary)
    except:
        temp.append("not fetched")
    
    for_df.append(temp)
    time.sleep(1)
# %%
final_df = pd.DataFrame(for_df, columns=["name", "fbid", "summary"])
# %%
genesnap = pd.read_csv("inputs/gene_snapshots_5_2_23.txt", sep="\t", header=4)
# %%
merged = final_df.merge(genesnap, how="inner", left_on="fbid", right_on="##FBgn_ID")
# %%
final_merged = merged[["name", "GeneName", "fbid", "gene_snapshot_text", "summary"]]
# %%
final_merged
# %%
final_merged.to_csv("outputs/neuropeptide_full_info_merged.csv", index=False)
# %%
#Now going to repeat most of the above with the receptors
npr = pd.read_csv("inputs/neuropeptide_receptors_dump_5_10_23_ya.csv")
# %%
#these are empty rows
npr.drop([38, 39, 40], inplace=True)
# %%
#38 neuropeptide receptors
short_names_r = tuple(npr["Short Name"].str.strip().tolist())
# %%
engine = create_engine('postgresql+psycopg2://flybase@chado.flybase.org/flybase')
connection = engine.connect()
sqlstring = '''select * from feature where feature.name in ''' + str(short_names_r) + ''' and feature.is_obsolete=False'''
# %%
df2 = pd.read_sql(text(sqlstring), connection)
# %%
returned_nprs = df2["name"].tolist()
# %%
missing_nprs = set(short_names_r).difference(set(returned_nprs))
# %%
tblr = dict(zip(df2['name'], df2['uniquename']))
# %%
for_dfr = []
for key, value in tblr.items():
    temp  = []
    temp.append(key)
    temp.append(value)
    response = requests.get(api_url+value)
    try:
        summary = response.json()["resultset"]['result'][0]['summary']
        temp.append(summary)
    except:
        temp.append("not fetched")
    
    for_dfr.append(temp)
    time.sleep(1)
# %%
final_dfr = pd.DataFrame(for_dfr, columns=["name", "fbid", "summary"])

# %%
#merged_r = final_dfr.merge(genesnap, how="inner", left_on="fbid", right_on="##FBgn_ID")
# %%
genesnapr = pd.read_csv("inputs/gene_snapshots_receptors_5_10_23.txt", sep="\t", header=4)
# %%
merged_r = final_dfr.merge(genesnapr, how="inner", left_on="fbid", right_on="##FBgn_ID")
# %%
final_merged_r = merged_r[["name", "GeneName", "fbid", "gene_snapshot_text", "summary"]]
# %%
final_merged_r.to_csv("outputs/neuropeptide_receptors_full_info_merged.csv", index=False)
# %%
