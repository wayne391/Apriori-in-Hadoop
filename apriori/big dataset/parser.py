import csv
unique_item = [];
prefix = ['1000', '5000', '20000', '75000']

for p in prefix:
    with open(p + "_transactions.txt", "w") as wf:
        with open(p + '//' + p + '-out1.csv', newline='') as f:
            reader = csv.reader(f, delimiter="\t")
            for i, line in enumerate(reader):
                transactoin_list = line[0].split(', ')[1:]
                unique_item = unique_item + transactoin_list
                unique_item = list(set(unique_item))
                wri_line = ",".join(transactoin_list) + '\n'
                wf.write(wri_line)
    with open(p + "_items.txt", "w") as wf:
        unique_item.sort()
        wri = "\n".join(unique_item)
        wf.write(wri)