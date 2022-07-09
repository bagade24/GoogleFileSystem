import json
import sys
import pickle
import socket            
import threading
import time
import os
import errno

## all user details
userdetails=dict()
serverport=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
clientport=-1
current_port=8090
current_user='x'
## login details
backup_master=5999
master=6000
curr_server=-1

def login():
	global current_user
	global clientport
	global current_port
	username=input('Enter your username ')
	password=input('enter your password ')
 	
	if userdetails.get(username)==None:
		print("Incorrect username ",username)
		return False

	if userdetails[username] != password:
		print("Incorrect Password")
		return False

	current_user=username
	clientport=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	clientport.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	clientport.bind(('127.0.0.1',int(current_port)))


	print("Sucessful login")
	return True

def signup():
	username=input('Enter your username ')
	password=input('enter your password ')

	if userdetails.get(username)!=None:
		print("Username already exists")
		return False

	userdetails[username]=password;
	print("Sucessful sign up")

	cwd = os.getcwd()
	dir = os.path.join(cwd,username)
	if not os.path.exists(dir):
		os.mkdir(dir)
	return True

def individial_chunks(chunkserver,chunk_port,dir,i,data,second_port):

	# time.sleep(2)
	print(chunk_port,'<--')
	try:
		chunkserver.connect(('127.0.0.1',int(chunk_port))) ## chunkserver's port
		chunkserver.send(('201 '+str(current_port)+' '+dir+' '+str(i)+' '+str(second_port)).encode())
		response=chunkserver.recv(1024).decode()
		print(response,'<--respnse')
		if response == '200':
			return
		chunkserver.sendall(data)
		chunkserver.close()
	except socket.error as err:
		print('Some error ',chunk_port,err)

def sendchunktoserver(dir,command):

	# pos=0
	newfile = open(dir, "rb")
	for i in range(1,len(command),2):
		data=newfile.read(1024)
		# print(data)
		chunkserver=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		chunkserver.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		# serverport.bind(('127.0.0.1',8090))
		# print(command[i],'##')
		# chunkserver.connect(('127.0.0.1',int(command[i]))) ## chunkserver's port
		# chunkserver.send(('201 '+str(current_port)+' '+dir+' '+str(i)).encode())
		# response=chunkserver.recv(1024).decode()
		# # print(response,'<--respnse')
		# if response == '200':
		# 	return
		# chunkserver.sendall(data)
		# chunkserver.close()
		# time.sleep(1)
		c_no=int((i+1)/2)
		timer = threading.Thread(target=individial_chunks,args=([chunkserver,command[i],dir,c_no,data,command[i+1]]))
		timer.start()
		timer.join()




def upload_file():
	print('upload file')
	filename=input('Enter file name ')
	cwd = os.getcwd()
	dir = os.path.join(cwd,filename)
	
	if not os.path.exists(dir):
		print('File does not exist')
		return


	file_size = os.path.getsize(dir)

	try:

		serverport=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		serverport.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		# serverport.bind(('127.0.0.1',8090))
		
		serverport.connect(('127.0.0.1',curr_server)) ## server's port can be master's port

		serverport.send(('upload '+dir+' '+str(file_size)+' '+current_user+' '+filename).encode())
		# 
		response=serverport.recv(1024).decode()
		print(response)
		command = response.split(" ")
		if command[0]=='402':
			print('File Already exists')
			serverport.close()	
			return

		if command[0]=='401':
			serverport.close()
			# print(response,'<--')
			sendchunktoserver(dir,command)





	except socket.error as err:
		# return 0
		print('Error : ',(err))

def writable_chunks(chunkserver,chunk_port,dir,filename,c_no,writable_file):
	print(chunk_port,'<--')

	
	try:
		chunkserver.connect(('127.0.0.1',int(chunk_port))) ## chunkserver's port
		chunkserver.send(('204 '+str(current_port)+' '+filename+' '+str(c_no)).encode())
		# response=chunkserver.recv(1024).decode()
		# print(response,'<--respnse')
		# if response == '200':
		# 	return
		output=chunkserver.recv(1024)
		writable_file.write(output)
		chunkserver.close()

	except socket.error as err:
		print('Some error ',chunk_port,err)


def getfromchunkserver(filename,command):
	
	cwd = os.getcwd()
	dir = os.path.join(current_user,filename)
	newfile = open(dir, "ab")

	for i in range(1,len(command)):
		chunkserver=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		chunkserver.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		timer = threading.Thread(target=writable_chunks,args=([chunkserver,command[i],dir,filename,i,newfile]))
		timer.start()
		# timer.join()


def download_file():
	# print('download file')
	filename=input('Enter file name ')
	cwd = os.getcwd()
	dir = os.path.join(cwd,filename)
	
	if not os.path.exists(dir):
		print('File does not exist')
		return


	try:
		serverport=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		serverport.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		# serverport.bind(('127.0.0.1',8090))
		serverport.connect(('127.0.0.1',curr_server)) ## server's port can be master's port
		serverport.send(('download '+dir+' '+current_user).encode())
		# 
		response=serverport.recv(1024).decode()
		print('202 ->',response)
		command = response.split(" ")
		if command[0]=='402':
			print('File does not exists')
			serverport.close()	
			return

		if command[0]=='403':
			print('File is locked')
			serverport.close()	
			return

		if command[0]=='401':
			serverport.close()
			getfromchunkserver(filename,command)
	except socket.error as err:
		print('line 207'+err)



def lock_file():

	filename=input('Enter file name ')
	cwd = os.getcwd()
	dir = os.path.join(cwd,filename)
	
	if not os.path.exists(dir):
		print('File does not exist')
		return

	serverport=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	serverport.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	serverport.connect(('127.0.0.1',curr_server)) ## server's port can be master's port
	serverport.send(('lock_file '+dir).encode())
	response=serverport.recv(1024).decode()
	print('237 ->',response)
	command=response.split(' ')

	if command[0]=='401':
		print('File locked')

	serverport.close()


def unlock_file():

	filename=input('Enter file name ')
	cwd = os.getcwd()
	dir = os.path.join(cwd,filename)
	
	if not os.path.exists(dir):
		print('File does not exist')
		return

	serverport=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	serverport.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	serverport.connect(('127.0.0.1',curr_server)) ## server's port can be master's port
	serverport.send(('unlock_file '+dir).encode())
	response=serverport.recv(1024).decode()
	command=response.split(' ')

	if command[0]=='401':
		print('File unlocked')

	serverport.close()






def setup():
	global userdetails
	

	try:
	    with open("logininfo.pickle", "rb") as file:
	    	userdetails = pickle.load(file)
	    	# print(userdetails)
	except EOFError:
		userdetails = dict()
		

def save():

	with open("logininfo.pickle", "wb") as file:
		pickle.dump(userdetails,file,pickle.HIGHEST_PROTOCOL)


def serverchecker():
	global master
	global curr_server
	global backup_master

	server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	try:
		server.bind(("127.0.0.1", master))
    
	except socket.error as e:

		if e.errno == errno.EADDRINUSE:

			curr_server=master
			server.close()
			return
    
	curr_server=backup_master              
	server.close()



def main():
	global clientport
	global current_port
	n = len(sys.argv)
	if n<1:
		return 0

	current_port=int(sys.argv[1])
	setup()

	while(1):

		serverchecker()

		command=input('>')
		if command=='login':
			login()

		if command=='signup':
			signup()

		if command=='exit':
			save()
			return 0

		if command=='upload_file':
			upload_file()

		if command=='download_file':
			download_file()

		if command=='lock':
			lock_file()

		if command=='unlock':
			unlock_file()


if __name__ == '__main__':
	main()