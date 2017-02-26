def main():
	file = open('STEP1_OP.json')
	i=1
	op = list()
	while True:
		line = file.readline()
		line = line.split(',')
		if len(line) > 4:
			a = line[3]
			a = a[8:-1]
			l=''
			i = 0
			while i < len(str(a)):
				if a[i]=='#':
					if (i + 1) < len(str(a)):
						i+=1
						l += a[i]
						i+=1
						while i < len(str(a)) and a[i]!=' ':
							l += a[i]
							i += 1
						l = l.split('\\n')
						l= str(l[0])
						op.append(l.lower())
						l=''

				i += 1
		if len(line)<=1:
			break

	f1 = open('MAP_REDUCE_INPUT_1.txt','a')
	for i in op:	
		f1.write(str(i)+'\n')
	f1.close()

if __name__=='__main__':
	main()
