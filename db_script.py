import pandas as pd
import pymongo

client = pymongo.MongoClient("localhost:27017")
db = client["123"]
collection = db["devices"]
def filter():
    df = pd.read_csv("https://raw.githubusercontent.com/Ajaykolluri/files/main/raw_data.csv")
    dff = df.sort_values(["device_fk_id","time_stamp"],ascending=False)
    return dff

def dbInsert():
    dff = filter()
    final_df = dff.drop_duplicates(["device_fk_id"])
    print(final_df)
    return final_df
    
final_df = dbInsert()
deviceId = list(final_df["device_fk_id"])
data = final_df.to_dict(orient='records')
for i in deviceId:
    da = final_df.loc[final_df.device_fk_id == i].to_dict(orient="records")
    collection.update_one({"device_fk_id":i},{"$set":da[0]},upsert=True)
