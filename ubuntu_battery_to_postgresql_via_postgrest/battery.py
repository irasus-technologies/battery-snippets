#!/usr/bin/env python3

import datetime
import json
import pytz
import requests

def getMeasurement(name):
	filehandle = open('/sys/class/power_supply/BAT0/' + name, 'r')
	value = filehandle.read()
	filehandle.close()
	return value

def packageMeasurement():

	datapoint = {}

	datapoint['SoC'] = int(getMeasurement('capacity'))
	datapoint['capacity'] = float(getMeasurement('charge_now'))/1000000
	datapoint['voltage'] = float(getMeasurement('voltage_now'))/1000000
	if (getMeasurement('status').strip('\n') == 'Discharging'):
		datapoint['currentDischarging'] = float(getMeasurement('current_now'))/1000000
		datapoint['currentCharging'] = 0
	else:
		datapoint['currentCharging'] = float(getMeasurement('current_now'))/1000000
		datapoint['currentDischarging'] = 0

	IN = pytz.timezone('Asia/Kolkata')
	datapoint['timestamp'] = IN.localize(datetime.datetime.now()).isoformat()

	return datapoint

def postMeasurement(datapoint):

	url = "http://localhost:3000/basic_measurements_packs"
	datapoint['packNumber'] = 1
	payload = json.dumps(datapoint)
	headers = {
		'prefer': "return=representation",
		'content-type': "application/json",
		'cache-control': "no-cache"
	}
	return requests.request("POST", url, data=payload, headers=headers)

if __name__ == '__main__':
	try:
		measurement = packageMeasurement()
		response = postMeasurement(measurement)
	except:
		payload = {}
		payload['measurement'] = measurement
		filehandle = open('/home/anirudh/Code/battery.ConnectionRefusedError.log', 'a')
		filehandle.write(json.dumps(payload))
		filehandle.write('\n')
		filehandle.close()
	else:
		if (response.status_code != 201):
			payload = {}
			payload['status_code'] = response.status_code
			payload['text'] = response.text
			filehandle = open('/home/anirudh/Code/battery.201.log', 'a')
			filehandle.write(measurement)
			filehandle.write('\n')
			filehandle.close()