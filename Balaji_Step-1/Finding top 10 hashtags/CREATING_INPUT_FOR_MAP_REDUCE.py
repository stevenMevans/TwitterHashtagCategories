import re
f = open('STEP1_OP1.json')
wf= open('Hashtags_Extract.txt', 'w')
while True:
	ind = 0
	original_data = f.readline()
	j=1
	if len(original_data)<=1:
		break
	else:
		a = original_data.split(',')
		for i in a:
		#finding entries key
			if i.startswith('"entities"') and j>0:
				ind = original_data.index('"entities"')
				bb=original_data[ind:original_data.index('"urls"')]
				#extracting hashtags
				bb=bb[23:-1]
				c=bb.count('"text"')
				nn=len(bb)
				if c>0:
					ind = [m.start() for m in re.finditer('text', bb)]
					for i in ind:
						v = i + 6
						vv = bb[v:]
						vv = vv.split()
						vv = vv[0]
						op = vv[:vv.index(',')]
						print(op)
						j-=1
						wf.write(str(op)+'\n')
f.close()
wf.close()
		