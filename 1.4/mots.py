#!/usr/bin/python3

with open('part-r-00000', 'r') as content_file:
    contents = content_file.read()

max = 0
mot = 'erreur'

for line in contents.split("\n"):
	line = line.split("\t")
	n = int(line[1])

	if n > max:
		max = n
		mot = line[0]

print("Maximum atteint pour le mot '" + mot + "' present " + str(max) + " fois")
# Maximum atteint pour le mot 'de' present 16757 fois
