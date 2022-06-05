import random

myfile = open("Data.txt", "w") 

for i in range(100):
	bairro = random.choice(list(open('Bairros.txt')))
	bairro = bairro.rstrip()
	temperatura = random.uniform(-100,100)
	temperatura = "{:.2f}".format(temperatura)
	
	data = bairro + " " + str(temperatura)

	myfile.write(data)
	myfile.write('\n')

myfile.close()
