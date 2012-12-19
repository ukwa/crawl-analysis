
import sys, re
import csv
import math
from collections import defaultdict

def convert_bytes(bytes):
    bytes = float(bytes)
    if bytes >= 1099511627776:
        terabytes = bytes / 1099511627776
        size = '%.0fTB' % terabytes
    elif bytes >= 1073741824:
        gigabytes = bytes / 1073741824
        size = '%.0fGB' % gigabytes
    elif bytes >= 1048576:
        megabytes = bytes / 1048576
        size = '%.0fMB' % megabytes
    elif bytes >= 1024:
        kilobytes = bytes / 1024
        size = '%.0fKB' % kilobytes
    else:
        size = '%.0fB' % bytes
    return size

def trunc_bytes(bytes):
    if bytes == "0":
        return 0
    lb = math.log( float(bytes), 2.0 )
    lb = round(lb)
    lb = math.pow( 2.0, lb )
    return int(lb)

report_file = csv.reader( open(sys.argv[1], "rb"), delimiter='\t')

hg = defaultdict(int)

for row in report_file:
    v = trunc_bytes(row[1])
    #if row[2] == "1":
    hg[v] += 1

for i in hg.keys():
    print "{}\t{}\t{}".format( convert_bytes(i), i, hg[i] )

