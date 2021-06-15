import paho.mqtt.client as mqtt
from csv import DictReader

#  function
def connect_msg(client,username,flags,rc):
    print('Connected to broker with code:'+str(rc))


# function
def message_msg(client,userdata,msg):
    print(str(msg.payload.decode("utf-8")))

# function
def publish_msg():
    print('Message Published')

# Creating client
client = mqtt.Client('publisher-1')

# Connect to broker
client.connect("localhost",1883)

# Connecting callback functions
client.on_connect= connect_msg
client.on_publish = publish_msg()
client.on_message= message_msg

with open(r'testcases.csv',encoding='utf-8-sig') as read_obj:
        csv_dict_reader = DictReader(read_obj)
        for row in csv_dict_reader:
            data = f"^{row['Packet_type']}|{row['DeviceID']}|{row['SequenceNumber']}|{row['Lattitude']}|{row['Longitude']}|{row['Time']}|{row['Date']}|{row['Speed']}|{row['Heading']}|{row['NoOfSattelites']}|{row['BMSStatus']}|{row['AccelerometerMovement']}|{row['IgnitionStatus']}|{row['ImmobilizationStatus']}|{row['Reserved']}|{row['GPSOdometer']}|{row['Reserved']}|{row['GPSDataValidityStatus']}|{row['PacketLiveOrStoredStatus']}|{row['Reserved']}|{row['FirmwareVersion']}#"
            client.publish("iot\data",data)       

client.subscribe("data")

try:
    print("enter CTRL+C to exit")
    client.loop_forever()
except:
    print('disconnecting')

client.disconnect()


