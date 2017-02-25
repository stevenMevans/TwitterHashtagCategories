def main():
	file = open('STEP1_OP.json')
	i=1
	dic = dict()
	while True:
		# manipulating each line
		line = file.readline()
		line = line.split(',')
		if len(line) > 4:
			# storing text attribute of tweet in a
			a = line[3]
			words = str(line[3]).split()
			for e in words:
				# Finding words starting with '#' and if so then store in a dictionary (hash table)
				if e.startswith('#'):
					if e in dic.keys():
						dic[e] += 1
					else:
						dic[e] = 1
		if len(line)<=1:
			break
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
	
	op1 = o[10:]
	f1 = open('AFTER_TOP_10_TWEETS.txt','a')
	for i in op1:	
		f1.write(str(i)+'\n')
	f1.close()

if __name__=='__main__':
	main()
