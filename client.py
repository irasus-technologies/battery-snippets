import socket
from csv import DictReader
clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
clientSocket.connect(("192.168.43.13", 1337))
# open file in read mode   
with open(r'testcases.csv',encoding='utf-8-sig') as read_obj:
    # pass the file object to DictReader() to get the DictReader object
    csv_dict_reader = DictReader(read_obj)
    # iterate over each line as a ordered dictionary
    for row in csv_dict_reader:
        data = f"^{row['Packet_type']}|{row['DeviceID']}|{row['SequenceNumber']}|{row['Lattitude']}|{row['Longitude']}|{row['Time']}|{row['Date']}|{row['Speed']}|{row['Heading']}|{row['NoOfSattelites']}|{row['BMSStatus']}|{row['AccelerometerMovement']}|{row['IgnitionStatus']}|{row['ImmobilizationStatus']}|{row['Reserved']}|{row['GPSOdometer']}|{row['Reserved']}|{row['GPSDataValidityStatus']}|{row['PacketLiveOrStoredStatus']}|{row['Reserved']}|{row['FirmwareVersion']}#"
        clientSocket.send(data.encode()) #sending data to node-red

        # below functions are checking the validity of sendt data
        # def SequenceNumber(num):
        #     if num <= 0:
        #         print("invalid sequence number")
        #         return False 
        #     return True   
        # SequenceNumber(float(row['SequenceNumber']))

        # def Time(num):
        #     o = []
        #     while num:
        #         o.append(num[:2])
        #         num = num[2:]
        #     if int(o[0])>23 or int(o[1])>59 or int(o[2])>59:
        #         print("invalid time format")
        #         return False
        #     return True  
        # Time((row['Time']))

        # def Date(num):
        #     o = []
        #     while num:
        #         o.append(num[:2])
        #         num = num[2:]
        #     if int(o[0])>31 or int(o[1])>12 or int(o[0])==00 or int(o[1])==00:
        #         print("invalid date format")
        #         return False
        #     return True
        # Date((row['Date'])) 

        # def Speed(num):
        #     if num <0:
        #         print("invalid speed value")
        #         return False
        #     return True  
        # Speed(float(row['Speed']))            
        
        # def Heading(num):
        #     if num>=0 and num<=360:
        #         return True 
        #     else:
        #         print("invalid heading value it must be in between 0 and 360")
        #     return False 
        # Heading(float(row['Heading']))        
        
        # def BMSStatus(num):
        #     if num==0 or num==1:
        #         return True
        #     else:
        #         print("invaid BMSStatus value it must be 0 or 1")
        #     return False  
        # BMSStatus(float(row['BMSStatus']))            
        
        # def AccelerometerMovement(num):
        #     if num==0 or num==1:
        #         return True 
        #     else:
        #         print("invalid AccelerometerMovement value it must be 0 or 1")
        #     return False 
        # AccelerometerMovement(float(row['AccelerometerMovement']))            
        
        # def IgnitionStatus(num):
        #     if num==0 or num==1:
        #         return True 
        #     else:
        #         print("invalid IgnitionStatus value it must be 0 or 1")
        #     return False   
        # IgnitionStatus(float(row['IgnitionStatus']))        
        
        # def ImmobilizationStatus(num):
        #     if num==0 or num==1:
        #         return True 
        #     else:
        #         print("invalid ImmobilizationStatus value it must be 0 or 1")
        #     return False 
        # ImmobilizationStatus(float(row['ImmobilizationStatus']))             
        
        # def GPSDataValidityStatus(num):
        #     if num==0 or num==1:
        #         return True 
        #     else:
        #         print("invalid GPSDataValidityStatus value it must be 0 or 1")
        #     return False  
        # GPSDataValidityStatus(float(row['GPSDataValidityStatus']))          
        
        # def PacketStatus(num):
        #     if num==0 or num==1:
        #         return True
        #     else:
        #         print("in valid PacketLiveOrStoredStatus value it must be 0 or 1")
        #     return False
        # PacketStatus(float(row['PacketLiveOrStoredStatus']))                    
        
        print(data)  #printing data that is send to nodered   
        #print("\n")


#code to receive data from server
while True:
   msg = clientSocket.recv(1024)
   print(msg.decode("utf-8"))
 
