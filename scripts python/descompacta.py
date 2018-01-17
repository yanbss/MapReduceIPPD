#python3 descompacta.py
import subprocess as p
import os

path = os.getcwd()

folders = os.listdir()
files_no_folder = ["changeFolder.py", "descompacta.py", "renameFiles.py"]

for file in files_no_folder:
	folders.remove(file)

number_folder = 0
total_folder = len(folders)

for folder in folders:
	#Entra de pasta em pasta 
	os.chdir(path + "/" + folder)
	bz2 = os.listdir()
	number_folder = number_folder + 1

	for b in bz2:
		p.run(["bunzip2", b])

	print(number_folder, "de", total_folder)
