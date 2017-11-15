def Euc(a,b):
    return abs((a-b)*(a-b))
def Man(a,b):
    return abs(a-b)

name = 'euc_c2/output_19/part-r-00000'
file = open(name, 'r', encoding='UTF-8')
line = file.readline();

print (line)
w,h = 58,10
Matrix = [[ 0 for x in range(w)] for y in range(h)]
r = []

for i in range(0,10):
    line = file.readline();
    data = line.split(" ")
    #print (len(data))
    for j in range(0,58):
        Matrix[i][j] = round(float(data[j]),10)

#ECU remember to pow 0.5 !!!!!!!!!!!

for i in range(0,10):
    for j in range(i+1,10):
        dist = 0
        print ("cluster "+str(i+1)+"  "+str(j+1))
        for k in range(0,58):
            dist += Euc(Matrix[i][k],Matrix[j][k])
        dist = pow(dist,0.5)
        print ("    "+str(round(dist,6)))
'''
# For ManHA 
for i in range(0,10):
    for j in range(i+1,10):
        dist = 0
        print ("cluster "+str(i+1)+"  "+str(j+1))
        for k in range(0,58):
            dist += Man(Matrix[i][k],Matrix[j][k])
        print ("    "+str(round(dist,6)))'''


file.close()
