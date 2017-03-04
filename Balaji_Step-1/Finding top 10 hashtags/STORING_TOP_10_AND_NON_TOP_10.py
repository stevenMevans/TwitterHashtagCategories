def main():
	f = open('MAP_REDUCE_OUTPUT.txt')
	hashtable = {}
	while True:
		inp = f.readline()
		l = inp.split()
		if len(l) == 2:
			hashtable[l[0]] = int(l[1])
		if len(inp)<=1:
			break
	d = hashtable
	o = list()
	s = [(k, d[k]) for k in sorted(d, key=d.get, reverse=True)]
	for k, v in s:
		o.append((k, v))
	
	op = o[0:10]
	f = open('TOP_10_HASGTAGS.txt','w')
	for i in op:	
		f.write(str(i)+'\n')
	f.close()
	
	op1 = o[10:]
	f1 = open('NON_TOP_10_HASHTAGS.txt','w')
	for i in op1:	
		f1.write(str(i)+'\n')
	f1.close()

if __name__=='__main__':
	main()
