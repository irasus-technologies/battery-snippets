import socket
from csv import DictReader
clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
clientSocket.connect(("192.168.43.113", 1337))
#open file in read mode   
with open(r'Etestcases.csv',encoding='utf-8-sig') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        data = f"^{row['Packet_type']}|{row['DeviceID']}|{row['SequenceNumber']}|{row['Time']}|{row['Date']}|{row['voltage']}|{row['Reserved']}|{row['current']}|{row['SoC']}|{row['max_voltage_cell_value']}|{row['max_voltage_cell_number']}|{row['min_voltage_cell_value']}|{row['min_voltage_cell_number']}|{row['max_temperature_cell_value']}|{row['max_temperature_cell_number']}|{row['min_temperature_cell_value']}|{row['min_temperature_cell_number']}|{row['SoQi']}|{row['SoCMOSFET']}|{row['SoDMOSFET']}|{row['reserved']}|{row['capacity']}|{row['reserve']}|{row['Temperature']}|{row['SoEC']}|{row['SoEL']}|{row['SoEB']}|{row['CycleCount']}|{row['equilibrium_1']}|{row['equilibrium_2']}|{row['equilibrium_3']}|{row['equilibrium_4']}|{row['equilibrium_5']}|{row['equilibrium_6']}|{row['failure_1']}|{row['failure_2']}|{row['failure_3']}|{row['failure_4']}|{row['failure_5']}|{row['failure_6']}|{row['failure_7']}|{row['failure_8']}|{row['Reserved']}|{row['Reserved']}#"   
        clientSocket.send(data.encode()) 
        print(data)  

while True:
    msg = clientSocket.recv(1024)
    print(msg.decode("utf-8"))