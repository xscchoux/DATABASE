import csv
import json
import sys
sys.path.append("..")

from src.TenGreatestHitters import TenGreatestHittersCSV
from src.TenGreatestHitters import TenGreatestHittersSQL

# Count Inversions in an array


file = open("TenGreatestHittersTest.txt","a")


s1 = TenGreatestHittersCSV()

print("start...")
result = s1.openfile()
print("end...")

file.write(" ========= Search Ten Greatest Hitters in a CSV File ==========\n")
for item in result:
	file.write(json.dumps(item)+'\n')




s2 = TenGreatestHittersSQL()

print("start...")
result2 = s2.run()
print("end...")

file.write("========== Search Ten Greatest Hitters in a MySQL =============\n")

for item in result2:
	file.write(str(item) + '\n')

