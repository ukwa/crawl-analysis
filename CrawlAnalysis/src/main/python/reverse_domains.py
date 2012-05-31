
import sys, re
import csv
import mimeparse
import math
from collections import defaultdict

report_file = csv.reader( open(sys.argv[1], "rb"), delimiter=' ')

hg = defaultdict(int)

p = re.compile( '\.www$')

for row in report_file:
    row[2] = '.'.join(row[2].split('.')[::-1])
    row[2] = p.sub('', row[2])
    print "\t".join(row)

#print "Year\tFirst Observed\tLast Observed"
#for i in hg.keys():
#    print "{}\t{}\t{}".format( convert_bytes(i), i, hg[i] )

