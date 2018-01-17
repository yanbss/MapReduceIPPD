import os
import subprocess as p

path = os.getcwd()

folders = os.listdir()
files_no_folder = ["changeFolder.py", "descompacta.py", "renameFiles.py"]

for file in files_no_folder:
	folders.remove(file)

directory = "../Wikipedia-database"
if not os.path.exists(directory):
	os.mkdir(directory)

number_folder = 0
total_folder = len(folders)

for folder in folders:
	#Entra de pasta em pasta 
	number_folder = number_folder + 1
	os.chdir(path + "/" + folder)
	p.run("mv * ../../Wikipedia-database", shell = True)
	print(number_folder, "de", total_folder)