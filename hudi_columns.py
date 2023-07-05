#
# hudi_columns = ['_hoodie_data1', '_hoodie_data2','data3']
# print(hudi_columns)
# hudi_columns = [i for i in hudi_columns if not i.startswith('_hoodie') ]
# print(hudi_columns)


a = ['111','222','333','444']
b = ['111','222']
extra = ['555']

c = [x for x in a if x  in b]+ extra
print(c)
print(type(c))
d = c.append('555')
print(type(d))