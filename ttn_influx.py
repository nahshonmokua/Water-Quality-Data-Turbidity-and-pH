import sys
import time
import ttn
import datetime as dt

from influxdb import InfluxDBClient

app_id = "wqm"
access_key = "ttn-account-v2.HEp-fHqzxm7bkt5712WWFHd8jqAtDZuhH0gE3Gb5EeM"

GTW_ID = 'eui-00800000a0002125' # gateway of interest

db_client = InfluxDBClient(host='35.247.79.208', port=8086, username='admin', password='do3uA2oR31253iSv', ssl=True)
db_client.create_database('wqm_db')
db_client.switch_database('wqm_db')

def uplink_callback(msg, client):

  print("Received uplink from ", msg.dev_id)

  influxdb_entry = {}

  influxdb_entry['time'] = msg.metadata.time
  fields = {}

  fields['data_rate'] = msg.metadata.data_rate


  for gtw in msg.metadata.gateways:
    if gtw.gtw_id == GTW_ID:
      fields['rssi'] = float(gtw.rssi)
      fields['snr'] = float(gtw.snr)

  try:
    fields['Temperature'] = float(msg.payload_fields.temperature_4)
           
  except:
    pass

  try:
    fields['pH'] = float(msg.payload_fields.analog_in_3)
    fields['Turbidity'] = float(msg.payload_fields.analog_in_2)
  except:
    pass

  influxdb_entry['fields'] = fields
  influxdb_entry['measurement'] = 'wqm_data'
  influxdb_entry['tags'] = {'sensor': msg.dev_id}

  print(influxdb_entry)


  db_client.write_points([influxdb_entry])

handler = ttn.HandlerClient(app_id, access_key)

# using mqtt client
mqtt_client = handler.data()
mqtt_client.set_uplink_callback(uplink_callback)
mqtt_client.connect()

while True:
  try:
    time.sleep(60)
  except KeyboardInterrupt:
    print('Closing ...')
    mqtt_client.close()
    sys.exit(0)

