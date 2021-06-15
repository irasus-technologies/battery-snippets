import socket
from csv import DictReader
clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
clientSocket.connect(("192.168.43.113", 1337))
#open file in read mode   
with open(r'Ecopy.csv',encoding='utf-8-sig') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        data = f"^{row['Packet_type']}|{row['DeviceID']}|{row['SequenceNumber']}|{row['Time']}|{row['Date']}|{row['BMSHardwareVersion']}|{row['BMSSoftwareVersion']}|{row['SeriesCellQty']}|{row['TotalVoltage']}|{row['TotalCurrent']}|{row['ResidualCapacity']}|{row['NominalCapacity']}|{row['CycleLife']}|{row['ProductDate']}|{row['BalanceStatus']}|{row['ProtectionStatus']}|{row['RSOC']}|{row['FETControlStatus']}|{row['NTCCount']}|{row['NTCValue1']}|{row['NTCValue2']}|{row['NTCValuetillNTCCount']}|{row['CellVoltage1']}|{row['CellVoltage2']}|{row['CellVoltage3']}|{row['--tillNumberofSeriesCellQty']}|#"   
        clientSocket.send(data.encode()) 
        print(data)  

while True:
     msg = clientSocket.recv(1024)
     print(msg.decode("utf-8"))