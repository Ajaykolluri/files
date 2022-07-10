import pandas as pd
import pymongo

client = pymongo.MongoClient("localhost:27017")
db = client["12"]
collection = db["devices"]
def filter():
    df = pd.read_csv("https://raw.githubusercontent.com/Ajaykolluri/files/main/raw_data.csv")
    dff = df.sort_values(["device_fk_id","time_stamp"],ascending=False)
    return dff

def dbInsert():
    dff = filter()
    final_df = dff.drop_duplicates(["device_fk_id"])
    return final_df
final_df = dbInsert()
data = final_df.to_dict(orient='records')
collection.insert_many(data)