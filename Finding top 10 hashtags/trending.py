def main():
	file = open('STEP1_OP.json')
	i=1
	dic = dict()
	while True:
		line = file.readline()
		#print(line)

		line = line.split(',')
		if len(line) > 4:
			#print(line[3])
			a = line[3]
			#print(a[8:-1])
			words = str(line[3]).split()
			for e in words:
				if e.startswith('#'):
					#print(e)
					if e in dic.keys():
						dic[e] += 1
					else:
						dic[e] = 1
		if len(line)<=1:
			break
	#print(dic)

	d = dic
	o = list()
	s = [(k, d[k]) for k in sorted(d, key=d.get, reverse=True)]
	for k, v in s:
		o.append((k, v))
	
	op = o[0:10]
	f = open('TOP_10_TWEETS.txt','a')
	for i in op:	
		f.write(str(i)+'\n')
	f.close()
	

if __name__=='__main__':
	main()
