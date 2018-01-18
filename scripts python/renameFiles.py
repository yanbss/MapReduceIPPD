import os 
import subprocess as p
from random import randrange

path = os.getcwd()

folders = os.listdir()
files_no_folder = ["changeFolder.py", "descompacta.py", "renameFiles.py"]

for file in files_no_folder:
	folders.remove(file)

number = 0
number_folder = 0
total_folder = len(folders)

for folder in folders:
	#Entra de pasta em pasta 
	os.chdir(path + "/" + folder)
	files = os.listdir()
	number_folder = number_folder + 1
	for file in files:
		os.rename(file, "wiki" + str(number) )
		number = number+1
	print(number_folder, "de", total_folder)
