import serial

usb0device = serial.Serial('/dev/ttyUSB0', 9600, timeout = 0)
usb1device = serial.Serial('/dev/ttyUSB1', 9600, timeout = 0)

usb0log = open('usb0.log', 'a')
usb1log = open('usb1.log', 'a')

while True:
	while usb0device.in_waiting != 0:
		c = usb0device.read()
		usb1device.write(c)
		usb0log.write(c)
	while usb1device.in_waiting != 0:
		c = usb1device.read()
		usb0device.write(c)
		usb1log.write(c)
