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
		with open(file, "r") as fin:
			data = fin.read().splitlines(True)
			newname = ''.join(data[1:2]).split('\n')[0]
			try:
				newname = newname.replace(" ", "_")
			except:
				continue
			try:
				os.rename(file, newname)
			except:
				os.rename(file, file + str(randrange(0,1000)) )
				number = number+1
	print(number_folder, "de", total_folder)
print("Arquivos com nomes aleatorios: " + number)