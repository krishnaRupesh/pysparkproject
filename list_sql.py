
a = [1,2,3,4,5,6,78,9]
runid = "adafa-fadad-fafafrdsfad"

b = f"update audit.cdc_vendor set run_id = {runid} where id in {tuple(a)}"

print(b)

output = f"({str(a)[1:-1]})"
print(str(a)[1:-1])
print(output)

# import datetime
#
# a = datetime.datetime.now().replace(microsecond=0)