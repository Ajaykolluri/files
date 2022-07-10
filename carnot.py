import datetime
import pandas as pd
from flask import Flask, jsonify,request
from flask_restful import Resource, Api
from flask_cors import CORS
from db_script import filter,dbInsert

app = Flask(__name__)
CORS(app)

@app.route("/devices/getLatest", methods = ["GET"])
def  latest():
    try:
        body = request.args.get("deviceId")
        data = dbInsert()
        df = data.loc[data.device_fk_id == int(body)]
        if len(df) > 0:
            out = df.to_dict(orient="records")
            return jsonify(
                        {"status_code": 200,
                            "message": "latest device data fecthed successfully",
                            "data": out
                        }),200
        else:
            return jsonify(
                        {   "status_code": 404,
                            "message": "No devices found",
                            "data": []
                        }),404
    except Exception as err:
        return jsonify(
                        {"status_code": 500,
                            "message": "Error: "+ str(err),
                        }),500

@app.route("/devices/getLatLong", methods = ["GET"])
def  latlong():
    try:
        body = request.args.get("deviceId")
        data = filter()
        df = data.loc[data.device_fk_id == int(body)].reset_index()
        df[["time_stamp","sts"]] = df[["time_stamp","sts"]].apply(pd.to_datetime,format="%Y-%m-%d %H:%M:%S.%f")
        df["location"] = df[["latitude","longitude"]].apply(tuple,axis=1)
        out = {
                "deviceId":body,
                "startLocation": df["location"].iloc[-1],
                "endLocation" : df["location"].iloc[0]
                }
        print(out)
        if len(df) > 0:
            return jsonify({
                            "status_code": 200,
                            "message": "location of device data fecthed successfully",
                            "data": out
                            }),200
        else:
            return jsonify(
                        {   "status_code": 404,
                            "message": "No devices found",
                            "data": []
                        }),404
    except Exception as err:
        return jsonify(
                        {"status_code": 500,
                            "message": "Error: "+ str(err),
                        }),500

@app.route("/devices/getLatLongTime", methods = ["POST"])
def  timePeriod():
    try:
        body = request.get_json()
        deviceId = body["deviceId"]
        startTime = body["startTime"]
        endTime = body["endTime"]
        startTime = datetime.datetime.strptime(startTime,"%Y-%m-%dT%H:%M:%SZ")
        endTime = datetime.datetime.strptime(endTime,"%Y-%m-%dT%H:%M:%SZ")
        df = filter()
        df[["time_stamp"]] = df[["time_stamp"]].apply(pd.to_datetime,format="%Y-%m-%dT%H:%M:%SZ")
        df["location"] = df[["latitude","longitude","time_stamp"]].apply(list,axis=1)
        df = df.loc[(df.device_fk_id==deviceId)&(df.time_stamp>=startTime)&(df.time_stamp<=endTime)]
        df = df[["device_fk_id","location"]]
        if len(df) > 0:
            out = df.to_dict(orient="records")
            return jsonify(
                        {"status_code": 200,
                            "message": "latest device data fecthed successfully",
                            "data": out
                        }),200
        else:
            return jsonify(
                        {   "status_code": 404,
                            "message": "No devices found",
                            "data": []
                        }),404
    except Exception as err:
        return jsonify(
                        {"status_code": 500,
                            "message": "Error: "+ str(err),
                        }),500

if __name__ == '__main__':
    app.run(debug=False,port=6000)