import os 
import subprocess as p

path = os.getcwd()

dirpath, dirnames, filenames = os.walk(path)

folders = dirpath[1]

for folder in folders:
	#Entra de pasta em pasta 
	os.chdir(path + "/" + folder)
	files = os.listdir()
	for file in files:
		with open(file, "r") as fin:
			data = fin.read().splitlines(True)
			newname = ''.join(data[1:2]).split('\n')[0]
			try:
				newname = newname.replace(" ", "_")
			except:
				continue
		os.rename(file, newname)