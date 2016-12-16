#!/bin/python3

from pathlib import Path
contents = Path("part-r-00000").read_text()

max = 0
mot = 'erreur'

for line in contents.split("\n"):
	line = line.split("\t")
	n = int(line[1])

	if n > max:
		max = n
		mot = line[0]

print("Maximum atteint pour le mot '" + mot + "' présent " + str(max) + " fois")
# Maximum atteint pour le mot 'de' présent 16757 fois
