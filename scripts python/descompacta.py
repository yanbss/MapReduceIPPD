#python3 descompacta.py
import subprocess as p
import os

path = os.getcwd()

dirpath, dirnames, filenames = os.walk(path)

folders = dirpath[1]

for folder in folders:
	#Entra de pasta em pasta 
	os.chdir(path + "/" + folder)
	bz2 = os.listdir()
	
	for b in bz2:
		p.run(["bunzip2", b])
	