
import sys, re
import csv
import mimeparse
import math
from collections import defaultdict

report_file = csv.reader( open(sys.argv[1], "rb"), delimiter='\t')

for row in report_file:
    print row[2]

