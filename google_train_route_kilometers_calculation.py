# This script is used to get the kilometers from Google API by passing the "From station Names" and "To station names".
# After geeting kilometers from google API, script will update the kilometers value in table
# Script Process steps below:
# Step1: Select Query: To look only for new requests and assume if kilometers not found (for any different reason) in previous run we want to ignore those results to find again as new request. Because we will investigate why it was not found in initial request
# Step2: From the Selected rows, do FOR loop
# Step3: Each row take "From Station" (origins) and "To Station" (Destinations) and pass these values to function (kilometers) to get the kilometer value.
# Step4: Function will retrun the kilometer value
# Step5: Update the kilometer and current datetime in table and final commit

import datetime
import sys
import pyodbc 
import urllib.request
import json
import requests
import urllib.parse as ulprs


def kilometers(origins_par,destinations_par):
    endpoint = 'https://maps.googleapis.com/maps/api/distancematrix/json?'
    api_key = 'xxxxxxxxxxxxxx'
    transit_mode = "train"
    mode="transit"
    vehicle_type="RAIL"
    origins_stage = origins_par
    destinations_stage = destinations_par
    origins = ulprs.quote(origins_stage).replace('%2B','+').replace('%2C',',').replace('%28','(').replace('%29',')').replace('%27','\'')
    destinations = ulprs.quote(destinations_stage).replace('%2B','+').replace('%2C',',').replace('%28','(').replace('%29',')').replace('%27','\'')
    nav_request = 'origins={}&destinations={}&transit_mode={}&mode={}&vehicle.type={}&key={}'.format(origins, destinations, transit_mode, mode, vehicle_type, api_key)
    request = endpoint + nav_request
    response = urllib.request.urlopen(request).read()
    result = json.loads(response)
    return result
    
date_update = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=SERVER_NAME;'
                      'Database=DATABASE_NAME;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()

rows_station = cursor.execute("select station_name_origin,station_name_destination from TABLE WHERE google_kilometers_stations IS NULL AND km_updated_date_stations IS NULL").fetchall()

for row in rows_station:
    fn_result = kilometers(row.station_name_origin, row.station_name_destination)
    i_access = fn_result.get("rows",'NULL')
    if i_access == 'NULL':
        print('YESS')
    for i in fn_result["rows"]:
        for j in i["elements"]:
            if j["status"] != 'ZERO_RESULTS':
                output_access = j.get("distance",'NULL')
                if output_access != 'NULL':
                    output = j["distance"]["text"]
                    dest_address = fn_result['destination_addresses'][0]
                    orig_address = fn_result['origin_addresses'][0]
                    km = output.replace('km','')
                    cursor = cursor.execute("UPDATE TABLE SET google_kilometers_stations = ?, km_updated_date_stations = ?, google_origin_addresses_stations = ?, google_destination_addresses_stations = ?  WHERE station_name_origin = ? AND station_name_destination = ? ",km, date_update, orig_address, dest_address, row.station_name_origin, row.station_name_destination)
                    cursor.commit()
                else:
                    cursor = cursor.execute("UPDATE TABLE SET google_kilometers_stations = ?, km_updated_date_stations = ? WHERE station_name_origin = ? AND station_name_destination = ? ",'NOT_FOUND', date_update, row.station_name_origin, row.station_name_destination)
                    cursor.commit()
            if j["status"] == 'ZERO_RESULTS':
                cursor = cursor.execute("UPDATE TABLE SET google_kilometers_stations = ?, km_updated_date_stations = ?  WHERE station_name_origin = ? AND station_name_destination = ? ",'ZERO_RESULTS', date_update, row.station_name_origin, row.station_name_destination)                
                cursor.commit()

cursor.close()

del cursor
