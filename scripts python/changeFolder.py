import os
import subprocess as p

path = os.getcwd()

dirpath, dirnames, filenames = os.walk(path)

folders = dirpath[1]
directory = "../Wikipedia-database"
if not os.path.exists(directory):
	os.mkdir(directory)

for folder in folders:
	#Entra de pasta em pasta 
	os.chdir(path + "/" + folder)
	p.run("mv * ../../Wikipedia-database", shell = True)
	